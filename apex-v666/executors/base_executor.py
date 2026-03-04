"""
executors/base_executor.py
APEX PREDATOR NEO v666 – Executor Base de Arbitragem Triangular

Recebe oportunidades via Redis Pub/Sub, executa 3 pernas sequenciais
na Binance Spot, registra no Robin Hood Risk e aciona Auto-Earn.

Fluxo de execução:
 1. Recebe oportunidade → validação (latência, risco, lock)
 2. Perna 1: buy com quoteOrderQty (gasta USDT, recebe base)
 3. Perna 2: conversão intermediária
 4. Perna 3: volta para USDT
 5. Calcula PnL real (saldo final - inicial)
 6. Registra no Robin Hood
 7. Se lucro > threshold → Auto-Earn Hook
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict

from loguru import logger

from config.config import cfg
from core.auto_earn_hook import auto_earn
from core.binance_connector import connector
from core.robin_hood_risk import TradeRecord, robin_hood
from utils.redis_pubsub import redis_bus


class BaseExecutor:
    """Executor base para arbitragem triangular de 3 pernas."""

    def __init__(self, region: str) -> None:
        self.region = region
        self._running: bool = False
        self._total_executions: int = 0
        self._total_pnl: float = 0.0
        self._lock = asyncio.Lock()
        self._last_exec_ts: float = 0.0
        self._cooldown_s: float = 0.50  # 500ms mínimo entre execuções

    # ───────────────────────────────────────────────────────
    # INICIALIZAÇÃO
    # ───────────────────────────────────────────────────────
    async def start(self) -> None:
        """Registra handlers Redis e inicia escuta."""
        await redis_bus.subscribe(cfg.CH_OPPORTUNITIES, self._on_opportunity)
        await redis_bus.subscribe(cfg.CH_RISK, self._on_risk_alert)
        self._running = True
        logger.info(f"🎯 Executor [{self.region}] ativo e escutando oportunidades")

    # ───────────────────────────────────────────────────────
    # HANDLERS REDIS
    # ───────────────────────────────────────────────────────
    async def _on_opportunity(self, data: Dict[str, Any]) -> None:
        """Handler para oportunidades — filtra, valida e executa."""
        now = time.time()

        # Cooldown anti-spam
        if now - self._last_exec_ts < self._cooldown_s:
            return

        # Robin Hood deve permitir
        if not robin_hood.is_allowed:
            return

        # Não executar em paralelo (lock)
        if self._lock.locked():
            return

        async with self._lock:
            await self._execute_triangle(data)

    async def _on_risk_alert(self, data: Dict[str, Any]) -> None:
        """Handler para alertas de risco do Robin Hood."""
        if data.get("type") == "PAUSE":
            logger.warning(
                f"🚨 [{self.region}] Robin Hood ATIVADO: "
                f"{data.get('reason', '?')} | "
                f"Pausa {cfg.ROBIN_HOOD_COOLDOWN_S}s"
            )

    # ───────────────────────────────────────────────────────
    # EXECUÇÃO DE 3 PERNAS
    # ───────────────────────────────────────────────────────
    async def _execute_triangle(self, opp: Dict[str, Any]) -> None:
        """Executa as 3 pernas sequenciais de arbitragem."""
        opp_id = opp.get("id", "?")
        legs = opp.get("legs", [])
        capital = float(opp.get("capital_needed", 0))

        if len(legs) < 3 or capital <= 0:
            return

        # Limitar capital pelo Robin Hood
        max_size = robin_hood.max_order_size()
        capital = min(capital, max_size)
        if capital < 1.0:
            return

        t0 = time.time()
        logger.info(
            f"⚡ [{self.region}] Executando #{opp_id} | "
            f"{opp.get('path', '?')} | Capital: ${capital:.2f}"
        )

        # Snapshot de saldo antes
        balance_before = await connector.get_balance("USDT")

        executed_legs = []
        current_amount = capital

        try:
            for i, leg in enumerate(legs):
                sym = leg["symbol"]
                side = leg["side"]
                order = None

                # Perna 1 buy: usar quoteOrderQty (gastar X USDT)
                if i == 0 and side == "buy":
                    order = await connector.market_order(
                        sym, "buy", quote_qty=current_amount,
                    )
                else:
                    # Demais pernas: usar amount com precisão
                    amt = connector.to_amount_precision(sym, current_amount)
                    if amt <= 0:
                        logger.error(
                            f"Amount zero após precisão: {sym} ({current_amount})"
                        )
                        break
                    order = await connector.market_order(sym, side, amount=amt)

                if not order:
                    logger.error(f"❌ Perna {i+1}/3 FALHOU: {side} {sym}")
                    break

                filled = float(order.get("filled", 0))
                cost = float(order.get("cost", 0))

                # Atualizar amount para próxima perna
                current_amount = filled if side == "buy" else cost

                executed_legs.append({
                    "symbol": sym,
                    "side": side,
                    "filled": filled,
                    "cost": cost,
                    "avg_price": order.get("average"),
                })
                logger.info(
                    f"   ✅ Perna {i+1}/3: {side.upper()} {sym} | "
                    f"Filled: {filled} | Cost: {cost}"
                )

        except Exception as exc:
            logger.error(f"Erro durante execução: {exc}")

        # Calcular PnL real
        elapsed_ms = (time.time() - t0) * 1000
        balance_after = await connector.get_balance("USDT")
        real_pnl = balance_after - balance_before

        # Registrar no Robin Hood
        record = TradeRecord(
            triangle_id=opp_id,
            timestamp=time.time(),
            gross_profit=real_pnl + (capital * cfg.fee_3_legs),
            net_profit=real_pnl,
            capital_used=capital,
            legs_executed=len(executed_legs),
            duration_ms=elapsed_ms,
        )
        await robin_hood.record_trade(record)

        self._total_executions += 1
        self._total_pnl += real_pnl
        self._last_exec_ts = time.time()

        # Publicar resultado via Redis
        await redis_bus.publish(cfg.CH_EXECUTIONS, {
            "type": "EXECUTION_DONE",
            "region": self.region,
            "id": opp_id,
            "path": opp.get("path", ""),
            "legs_executed": len(executed_legs),
            "pnl": round(real_pnl, 6),
            "duration_ms": round(elapsed_ms, 1),
            "capital": capital,
        })

        emoji = "💰" if real_pnl > 0 else "📉"
        logger.info(
            f"{emoji} [{self.region}] #{opp_id} | "
            f"PnL: ${real_pnl:+.6f} | {elapsed_ms:.0f}ms | "
            f"{len(executed_legs)}/3 pernas"
        )

        # Auto-Earn Hook: mover lucro para Simple Earn
        if real_pnl >= cfg.AUTO_EARN_MIN_PROFIT:
            await auto_earn.process(real_pnl)

    # ───────────────────────────────────────────────────────
    # ESTATÍSTICAS
    # ───────────────────────────────────────────────────────
    def stats(self) -> Dict:
        return {
            "region": self.region,
            "executions": self._total_executions,
            "pnl": round(self._total_pnl, 6),
            "running": self._running,
        }
