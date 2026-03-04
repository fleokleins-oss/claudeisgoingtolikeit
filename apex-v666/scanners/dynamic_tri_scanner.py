"""
scanners/dynamic_tri_scanner.py
APEX PREDATOR NEO v666 – Scanner Dinâmico de Arbitragem Triangular

Descobre automaticamente TODOS os triângulos de 3 pernas lucrativos
na Binance Spot. Scan contínuo a cada 40-50ms com cálculo de profit
real após 3× taxas (maker+taker).

Fluxo por ciclo:
 1. Atualiza tickers batch (cache 150ms)
 2. Avalia cada triângulo com preços bid/ask
 3. Pré-filtra por profit mínimo
 4. Busca orderbooks sob demanda para candidatos
 5. Roda ConfluenceEngine (7 módulos)
 6. Publica melhor oportunidade via Redis

Grafo de adjacência para descoberta O(V²) em vez de O(V³).
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from loguru import logger

from config.config import cfg
from core.binance_connector import connector
from core.confluence_engine import confluence
from core.robin_hood_risk import robin_hood
from utils.redis_pubsub import redis_bus


# ═══════════════════════════════════════════════════════════
# ESTRUTURAS DE DADOS
# ═══════════════════════════════════════════════════════════
@dataclass
class TriangleLeg:
    """Uma perna do triângulo de arbitragem."""
    symbol: str     # Par da Binance (ex: ETHUSDT)
    side: str       # "buy" ou "sell"
    from_asset: str # Ativo de entrada
    to_asset: str   # Ativo de saída


@dataclass
class TriangleOpportunity:
    """Oportunidade de arbitragem triangular completa."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    legs: List[TriangleLeg] = field(default_factory=list)
    path: str = ""              # Texto legível: USDT→ETH→BNB→USDT
    gross_pct: float = 0.0      # Profit bruto antes de taxas
    net_pct: float = 0.0        # Profit líquido após 3× taxas
    net_usd: float = 0.0        # Profit em USD estimado
    confluence_score: float = 0.0
    capital: float = 0.0        # Capital a ser usado
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """Serializa para publicação Redis."""
        return {
            "id": self.id,
            "path": self.path,
            "legs": [
                {
                    "symbol": leg.symbol,
                    "side": leg.side,
                    "from": leg.from_asset,
                    "to": leg.to_asset,
                }
                for leg in self.legs
            ],
            "gross_pct": round(self.gross_pct, 5),
            "net_pct": round(self.net_pct, 5),
            "net_usd": round(self.net_usd, 6),
            "confluence_score": round(self.confluence_score, 1),
            "capital_needed": round(self.capital, 4),
            "timestamp": self.timestamp,
        }


# ═══════════════════════════════════════════════════════════
# SCANNER PRINCIPAL
# ═══════════════════════════════════════════════════════════
class DynamicTriScanner:
    """Scanner que descobre e avalia triângulos automaticamente."""

    def __init__(self) -> None:
        self._triangles: List[List[TriangleLeg]] = []
        self._running: bool = False
        self._total_scans: int = 0
        self._total_hits: int = 0
        self._tickers: Dict[str, Dict] = {}
        self._tickers_ts: float = 0.0

    # ═══════════════════════════════════════════════════════
    # FASE 1: DESCOBERTA DE TRIÂNGULOS (GRAFO)
    # ═══════════════════════════════════════════════════════
    async def discover(self) -> int:
        """Constroi grafo de pares e encontra todos os ciclos de 3 vértices.
        Retorna quantidade de triângulos únicos descobertos."""
        logger.info("🔍 Descobrindo triângulos possíveis...")
        t0 = time.time()
        markets = connector.markets

        # Construir grafo de adjacência
        # graph[A][B] = (symbol, side_para_ir_de_A_a_B)
        graph: Dict[str, Dict[str, tuple]] = {}
        all_assets = set(cfg.BASE_ASSETS) | set(cfg.QUOTE_ASSETS)

        for sym, market in markets.items():
            if not market.get("active") or not market.get("spot"):
                continue

            base = market.get("base", "")
            quote = market.get("quote", "")
            if not base or not quote:
                continue

            # Pelo menos um ativo deve ser de interesse
            if base not in all_assets and quote not in all_assets:
                continue

            # base→quote via SELL (vender base, receber quote)
            graph.setdefault(base, {})[quote] = (sym, "sell")
            # quote→base via BUY (gastar quote, receber base)
            graph.setdefault(quote, {})[base] = (sym, "buy")

        # Encontrar ciclos de 3: START → A → B → START
        found: List[List[TriangleLeg]] = []
        seen_keys: Set[frozenset] = set()

        for start in cfg.QUOTE_ASSETS:
            if start not in graph:
                continue
            for mid_a in graph[start]:
                if mid_a == start:
                    continue
                for mid_b in graph.get(mid_a, {}):
                    if mid_b == start or mid_b == mid_a:
                        continue
                    # Verificar se mid_b fecha o ciclo de volta ao start
                    if start not in graph.get(mid_b, {}):
                        continue

                    sym1, side1 = graph[start][mid_a]
                    sym2, side2 = graph[mid_a][mid_b]
                    sym3, side3 = graph[mid_b][start]

                    # Deduplicação por conjunto de símbolos
                    key = frozenset({sym1, sym2, sym3})
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    found.append([
                        TriangleLeg(sym1, side1, start, mid_a),
                        TriangleLeg(sym2, side2, mid_a, mid_b),
                        TriangleLeg(sym3, side3, mid_b, start),
                    ])

        self._triangles = found
        elapsed = time.time() - t0
        logger.success(
            f"✅ {len(found)} triângulos únicos descobertos em {elapsed:.3f}s | "
            f"Grafo: {len(graph)} nós"
        )

        # Mostrar primeiros exemplos
        for tri in found[:10]:
            path = (
                f"{tri[0].from_asset}→{tri[0].to_asset}→"
                f"{tri[1].to_asset}→{tri[2].to_asset}"
            )
            syms = f"({tri[0].symbol}, {tri[1].symbol}, {tri[2].symbol})"
            logger.info(f"   📐 {path} {syms}")

        if len(found) > 10:
            logger.info(f"   ... e mais {len(found) - 10} triângulos")

        return len(found)

    # ═══════════════════════════════════════════════════════
    # FASE 2: LOOP CONTÍNUO DE SCAN
    # ═══════════════════════════════════════════════════════
    async def run(self) -> None:
        """Loop principal: avalia triângulos a cada SCAN_INTERVAL_MS."""
        self._running = True
        interval_s = cfg.SCAN_INTERVAL_MS / 1000.0
        last_heartbeat = time.time()

        logger.info(
            f"🚀 Scanner v666 rodando | "
            f"{len(self._triangles)} triângulos | "
            f"Intervalo: {cfg.SCAN_INTERVAL_MS}ms | "
            f"Min profit: {cfg.MIN_PROFIT_PCT}% | "
            f"Min score: {cfg.MIN_CONFLUENCE_SCORE}"
        )

        while self._running:
            cycle_start = time.time()

            try:
                best = await self._scan_cycle()

                if best:
                    receivers = await redis_bus.publish(
                        cfg.CH_OPPORTUNITIES, best.to_dict()
                    )
                    logger.info(
                        f"🎯 #{best.id} | {best.path} | "
                        f"{best.net_pct:+.4f}% (${best.net_usd:+.6f}) | "
                        f"Score: {best.confluence_score:.0f} | "
                        f"Recv: {receivers}"
                    )

                # Heartbeat a cada 30 segundos
                now = time.time()
                if now - last_heartbeat > 30:
                    await redis_bus.heartbeat({
                        "scans": self._total_scans,
                        "hits": self._total_hits,
                        "triangles": len(self._triangles),
                        "risk": robin_hood.summary(),
                    })
                    last_heartbeat = now

            except Exception as exc:
                logger.error(f"Erro no ciclo de scan: {exc}")

            # Manter intervalo fixo de 40-50ms
            elapsed = time.time() - cycle_start
            sleep_time = max(0, interval_s - elapsed)
            await asyncio.sleep(sleep_time)

    def stop(self) -> None:
        """Para o loop de scan."""
        self._running = False
        logger.info("🛑 Scanner parado")

    # ═══════════════════════════════════════════════════════
    # CICLO ÚNICO DE AVALIAÇÃO
    # ═══════════════════════════════════════════════════════
    async def _scan_cycle(self) -> Optional[TriangleOpportunity]:
        """Avalia todos os triângulos e retorna o melhor qualificado."""
        if not self._triangles:
            return None
        if not robin_hood.is_allowed:
            return None

        # Atualizar tickers (cache 150ms)
        now = time.time()
        if now - self._tickers_ts > 0.15:
            self._tickers = await connector.fetch_all_tickers()
            self._tickers_ts = now

        best: Optional[TriangleOpportunity] = None
        best_combined_score: float = 0.0

        for tri in self._triangles:
            # Passo 1: Avaliação rápida com tickers (sem I/O)
            opp = self._quick_evaluate(tri)
            if not opp or opp.net_pct < cfg.MIN_PROFIT_PCT:
                continue

            # Passo 2: Buscar orderbooks para candidatos (I/O necessário)
            orderbooks: Dict[str, Dict] = {}
            for leg in tri:
                ob = await connector.fetch_orderbook(leg.symbol, 10)
                if ob:
                    orderbooks[leg.symbol] = ob

            # Passo 3: Rodar ConfluenceEngine completa
            tri_data = {
                "legs": [
                    {"symbol": leg.symbol, "side": leg.side}
                    for leg in tri
                ],
                "net_profit_pct": opp.net_pct,
            }
            conf_result = confluence.analyze(tri_data, orderbooks, self._tickers)

            if not conf_result.is_valid:
                continue

            opp.confluence_score = conf_result.score

            # Score combinado: profit × confluência
            combined = opp.net_pct * (conf_result.score / 100.0)
            if combined > best_combined_score:
                best_combined_score = combined
                best = opp

        self._total_scans += 1
        if best:
            self._total_hits += 1
        return best

    # ═══════════════════════════════════════════════════════
    # AVALIAÇÃO RÁPIDA (SEM I/O)
    # ═══════════════════════════════════════════════════════
    def _quick_evaluate(
        self, tri: List[TriangleLeg],
    ) -> Optional[TriangleOpportunity]:
        """Calcula profit teórico usando bid/ask dos tickers.
        Simulação: 1.0 unidade da moeda start → 3 pernas → valor final."""
        amount = 1.0
        fee = cfg.fee_per_leg
        path_parts = [tri[0].from_asset]

        for leg in tri:
            tk = self._tickers.get(leg.symbol, {})
            bid = float(tk.get("bid", 0) or 0)
            ask = float(tk.get("ask", 0) or 0)

            if bid <= 0 or ask <= 0:
                return None

            if leg.side == "buy":
                # Gastar amount de quote para comprar base
                amount = (amount / ask) * (1.0 - fee)
            else:
                # Vender amount de base para receber quote
                amount = (amount * bid) * (1.0 - fee)

            path_parts.append(leg.to_asset)

        # Profit líquido após 3 pernas com taxas
        net_pct = (amount - 1.0) * 100.0

        # Capital disponível
        cap = robin_hood.max_order_size()
        if cap < 1.0:
            return None

        opp = TriangleOpportunity()
        opp.legs = tri
        opp.path = " → ".join(path_parts)
        opp.gross_pct = net_pct + (cfg.fee_3_legs * 100.0)
        opp.net_pct = net_pct
        opp.capital = min(cap, cfg.MAX_POR_CICLO)
        opp.net_usd = opp.capital * (net_pct / 100.0)
        return opp

    # ═══════════════════════════════════════════════════════
    # ESTATÍSTICAS
    # ═══════════════════════════════════════════════════════
    def stats(self) -> Dict:
        return {
            "triangles": len(self._triangles),
            "scans": self._total_scans,
            "hits": self._total_hits,
            "hit_rate_pct": round(
                (self._total_hits / max(1, self._total_scans)) * 100, 2
            ),
            "running": self._running,
        }


# Singleton global
scanner = DynamicTriScanner()
