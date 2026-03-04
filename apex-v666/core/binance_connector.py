"""
core/binance_connector.py
APEX PREDATOR NEO v666 – Conector Binance unificado (REST + cache agressivo).

Responsabilidades:
 - Conexão ccxt async com modo testnet/live automático
 - Cache de tickers (150ms) e orderbooks (15ms) para evitar rate limit
 - Ordens market e limit IOC para execução de baixa latência
 - Precisão automática (amount_to_precision / price_to_precision)
 - Simple Earn API para Auto-Earn Hook (busca melhor APR + inscrição)
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple

import ccxt.async_support as ccxt
from loguru import logger

from config.config import cfg


class BinanceConnector:
    """Conector unificado Binance Spot com cache agressivo."""

    def __init__(self) -> None:
        self._exchange: Optional[ccxt.binance] = None
        self._markets: Dict[str, Any] = {}
        self._symbols: List[str] = []
        # Cache de tickers
        self._ticker_cache: Dict[str, Dict] = {}
        self._ticker_ts: float = 0.0
        # Cache de orderbook por símbolo
        self._ob_cache: Dict[str, Dict] = {}
        self._ob_ts: Dict[str, float] = {}

    # ═══════════════════════════════════════════════════════
    # CONEXÃO
    # ═══════════════════════════════════════════════════════
    async def connect(self) -> None:
        """Inicializa exchange ccxt com sandbox ou live."""
        opts = {
            "defaultType": "spot",
            "adjustForTimeDifference": True,
            "recvWindow": 5000,
            "enableRateLimit": True,
            "rateLimit": 50,
        }
        if cfg.TESTNET:
            opts["sandboxMode"] = True

        self._exchange = ccxt.binance({
            "apiKey": cfg.api_key,
            "secret": cfg.api_secret,
            "options": opts,
            "timeout": 10000,
        })
        if cfg.TESTNET:
            self._exchange.set_sandbox_mode(True)

        self._markets = await self._exchange.load_markets(reload=True)
        self._symbols = [
            s for s, m in self._markets.items()
            if m.get("active") and m.get("spot")
        ]
        modo = "🟢 TESTNET" if cfg.TESTNET else "🔴 PRODUÇÃO LIVE"
        logger.success(f"✅ Binance [{modo}] — {len(self._symbols)} pares ativos")

    async def disconnect(self) -> None:
        """Desconexão limpa."""
        if self._exchange:
            await self._exchange.close()
            logger.info("🔌 Binance desconectada")

    # ═══════════════════════════════════════════════════════
    # PROPRIEDADES
    # ═══════════════════════════════════════════════════════
    @property
    def markets(self) -> Dict[str, Any]:
        return self._markets

    @property
    def symbols(self) -> List[str]:
        return self._symbols

    def symbol_exists(self, symbol: str) -> bool:
        m = self._markets.get(symbol)
        return m is not None and m.get("active", False)

    def get_market(self, symbol: str) -> Optional[Dict]:
        return self._markets.get(symbol)

    # ═══════════════════════════════════════════════════════
    # TICKERS BATCH (para scanner)
    # ═══════════════════════════════════════════════════════
    async def fetch_all_tickers(self) -> Dict[str, Dict]:
        """Busca todos os tickers em chamada REST única.
        Cache de 150ms evita rate limit sem perder atualidade."""
        now = time.time()
        if now - self._ticker_ts < 0.15 and self._ticker_cache:
            return self._ticker_cache
        try:
            self._ticker_cache = await self._exchange.fetch_tickers()
            self._ticker_ts = now
        except Exception as exc:
            logger.debug(f"fetch_tickers falhou: {exc}")
        return self._ticker_cache

    # ═══════════════════════════════════════════════════════
    # ORDER BOOK
    # ═══════════════════════════════════════════════════════
    async def fetch_orderbook(self, symbol: str, limit: int = 10) -> Optional[Dict]:
        """Order book com cache de 15ms por símbolo."""
        now = time.time()
        cached_ts = self._ob_ts.get(symbol, 0)
        if now - cached_ts < 0.015 and symbol in self._ob_cache:
            return self._ob_cache[symbol]
        try:
            ob = await self._exchange.fetch_order_book(symbol, limit)
            self._ob_cache[symbol] = ob
            self._ob_ts[symbol] = now
            return ob
        except Exception as exc:
            logger.debug(f"OB {symbol}: {exc}")
            return self._ob_cache.get(symbol)

    # ═══════════════════════════════════════════════════════
    # SALDO
    # ═══════════════════════════════════════════════════════
    async def get_balance(self, asset: str = "USDT") -> float:
        """Saldo disponível de um ativo específico."""
        try:
            bal = await self._exchange.fetch_balance()
            return float(bal.get("free", {}).get(asset, 0))
        except Exception as exc:
            logger.error(f"Erro saldo {asset}: {exc}")
            return 0.0

    async def get_all_balances(self) -> Dict[str, float]:
        """Todos os saldos livres > 0."""
        try:
            bal = await self._exchange.fetch_balance()
            return {
                k: float(v)
                for k, v in bal.get("free", {}).items()
                if float(v) > 0
            }
        except Exception as exc:
            logger.error(f"Erro saldos: {exc}")
            return {}

    # ═══════════════════════════════════════════════════════
    # ORDENS
    # ═══════════════════════════════════════════════════════
    async def market_order(
        self,
        symbol: str,
        side: str,
        amount: float = None,
        quote_qty: float = None,
    ) -> Optional[Dict]:
        """Ordem market. Se quote_qty fornecido em buy, usa quoteOrderQty."""
        try:
            params = {}
            if quote_qty and side == "buy":
                params["quoteOrderQty"] = quote_qty
                amount = None
            order = await self._exchange.create_order(
                symbol=symbol,
                type="market",
                side=side,
                amount=amount,
                params=params,
            )
            filled = order.get("filled", 0)
            avg = order.get("average", 0)
            cost = order.get("cost", 0)
            logger.info(
                f"📋 {side.upper()} {symbol} | "
                f"Filled: {filled} | Avg: {avg} | Cost: {cost}"
            )
            return order
        except Exception as exc:
            logger.error(f"❌ Ordem {side} {symbol}: {exc}")
            return None

    async def limit_ioc(
        self, symbol: str, side: str, amount: float, price: float,
    ) -> Optional[Dict]:
        """Limit IOC (Immediate-Or-Cancel) — para HFT agressivo."""
        try:
            return await self._exchange.create_order(
                symbol=symbol,
                type="limit",
                side=side,
                amount=amount,
                price=price,
                params={"timeInForce": "IOC"},
            )
        except Exception as exc:
            logger.error(f"❌ IOC {side} {symbol}@{price}: {exc}")
            return None

    # ═══════════════════════════════════════════════════════
    # PRECISÃO
    # ═══════════════════════════════════════════════════════
    def to_amount_precision(self, symbol: str, amount: float) -> float:
        """Ajusta amount para a precisão exigida pelo par."""
        try:
            return float(self._exchange.amount_to_precision(symbol, amount))
        except Exception:
            return amount

    def to_price_precision(self, symbol: str, price: float) -> float:
        """Ajusta preço para a precisão exigida pelo par."""
        try:
            return float(self._exchange.price_to_precision(symbol, price))
        except Exception:
            return price

    def min_order(self, symbol: str) -> Tuple[float, float]:
        """Retorna (min_amount, min_cost) de um par da Binance."""
        m = self._markets.get(symbol, {})
        lim = m.get("limits", {})
        return (
            float(lim.get("amount", {}).get("min", 0) or 0),
            float(lim.get("cost", {}).get("min", 0) or 0),
        )

    # ═══════════════════════════════════════════════════════
    # SIMPLE EARN (Auto-Earn Hook)
    # ═══════════════════════════════════════════════════════
    async def get_earn_products(self, asset: str = "USDT") -> List[Dict]:
        """Lista produtos Simple Earn flexíveis, ordenados por APR desc."""
        try:
            resp = await self._exchange.sapi_get_simple_earn_flexible_list(
                params={"asset": asset, "size": 20}
            )
            rows = resp.get("rows", [])
            rows.sort(
                key=lambda x: float(x.get("latestAnnualPercentageRate", 0)),
                reverse=True,
            )
            return rows
        except Exception as exc:
            logger.debug(f"Earn list {asset}: {exc}")
            return []

    async def subscribe_earn(self, product_id: str, amount: float) -> Optional[Dict]:
        """Inscreve valor no Simple Earn flexível de maior APR."""
        try:
            resp = await self._exchange.sapi_post_simple_earn_flexible_subscribe(
                params={"productId": product_id, "amount": str(amount)}
            )
            logger.success(
                f"💰 Earn inscrito: {amount} USDT → produto {product_id}"
            )
            return resp
        except Exception as exc:
            logger.error(f"Earn subscribe falhou: {exc}")
            return None


# Singleton global
connector = BinanceConnector()
