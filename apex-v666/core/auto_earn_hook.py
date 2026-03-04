"""
core/auto_earn_hook.py
APEX PREDATOR NEO v666 – Auto-Earn Hook

Após cada ciclo lucrativo > US$ 0.10:
 1. Busca o produto Simple Earn de MAIOR APR atual
 2. Move o lucro automaticamente via API Binance
 3. Publica confirmação via Redis para tracking

Cache de produtos: 5 minutos para evitar spam na API.
"""
from __future__ import annotations

import time
from typing import Dict, Optional

from loguru import logger

from config.config import cfg
from core.binance_connector import connector
from utils.redis_pubsub import redis_bus


class AutoEarnHook:
    """Hook pós-trade que inscreve lucro no Simple Earn Binance."""

    def __init__(self) -> None:
        self._total_earned: float = 0.0       # Total de lucro bruto processado
        self._total_subscribed: float = 0.0   # Total efetivamente inscrito
        self._sub_count: int = 0              # Número de inscrições
        self._product_cache: Optional[Dict] = None
        self._cache_expiry: float = 0.0       # Timestamp de expiração do cache

    # ───────────────────────────────────────────────────────
    # PROCESSAMENTO DE LUCRO
    # ───────────────────────────────────────────────────────
    async def process(self, net_profit: float, asset: str = "USDT") -> bool:
        """Processa lucro pós-trade. Se >= threshold, inscreve no Earn."""
        self._total_earned += net_profit

        # Verificar threshold mínimo
        if net_profit < cfg.AUTO_EARN_MIN_PROFIT:
            logger.debug(
                f"💤 Earn: ${net_profit:.4f} < mínimo ${cfg.AUTO_EARN_MIN_PROFIT:.2f} — ignorado"
            )
            return False

        logger.info(f"🏦 Auto-Earn: processando ${net_profit:.4f} {asset}")

        # Buscar melhor produto (com cache de 5 min)
        product = await self._get_best_product(asset)
        if not product:
            logger.warning(f"Nenhum produto Simple Earn disponível para {asset}")
            return False

        product_id = product.get("productId", "")
        apr = float(product.get("latestAnnualPercentageRate", 0))

        # Inscrever no Simple Earn
        result = await connector.subscribe_earn(product_id, net_profit)
        if not result:
            logger.error(f"Falha ao inscrever ${net_profit:.4f} no Earn")
            return False

        self._total_subscribed += net_profit
        self._sub_count += 1

        # Publicar confirmação via Redis
        await redis_bus.publish(cfg.CH_EARN, {
            "type": "SUBSCRIBED",
            "amount": round(net_profit, 6),
            "asset": asset,
            "product_id": product_id,
            "apr_pct": round(apr * 100, 2),
            "total_subscribed": round(self._total_subscribed, 4),
            "sub_count": self._sub_count,
        })

        logger.success(
            f"✅ Earn: ${net_profit:.4f} {asset} → Simple Earn "
            f"(APR {apr*100:.2f}%) | Total inscrito: ${self._total_subscribed:.4f} "
            f"({self._sub_count} inscrições)"
        )
        return True

    # ───────────────────────────────────────────────────────
    # CACHE DE PRODUTO
    # ───────────────────────────────────────────────────────
    async def _get_best_product(self, asset: str) -> Optional[Dict]:
        """Retorna melhor produto Simple Earn (cache 5 min)."""
        now = time.time()
        if self._product_cache and now < self._cache_expiry:
            return self._product_cache

        products = await connector.get_earn_products(asset)
        if not products:
            return None

        # Primeiro da lista já é o de maior APR (ordenado no connector)
        self._product_cache = products[0]
        self._cache_expiry = now + 300  # 5 minutos
        return self._product_cache

    # ───────────────────────────────────────────────────────
    # RESUMO
    # ───────────────────────────────────────────────────────
    def summary(self) -> Dict:
        return {
            "earned": round(self._total_earned, 4),
            "subscribed": round(self._total_subscribed, 4),
            "count": self._sub_count,
        }


# Singleton global
auto_earn = AutoEarnHook()
