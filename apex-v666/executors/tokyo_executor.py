"""
executors/tokyo_executor.py
APEX PREDATOR NEO v666 – Executor Tokyo

Especialização (executor backup, mais tolerante):
 - Latência máxima aceita: 100ms (compensa distância com profit maior)
 - Profit mínimo exigido: 0.12% (justifica a latência extra)
 - Confluence score mínimo: 65 (padrão do sistema)
 - Funciona como backup quando Singapore rejeita por latência
"""
from __future__ import annotations

from typing import Any, Dict

from loguru import logger

from executors.base_executor import BaseExecutor


class TokyoExecutor(BaseExecutor):
    """Executor backup — mais tolerante em latência, exige mais profit."""

    MAX_LATENCY_MS: float = 100.0
    MIN_PROFIT_PCT: float = 0.12
    MIN_CONFLUENCE_SCORE: float = 65.0

    def __init__(self) -> None:
        super().__init__(region="tokyo")

    async def _on_opportunity(self, data: Dict[str, Any]) -> None:
        """Override: filtro latência + profit + score."""
        # Filtro 1: latência
        latency_us = data.get("_latency_us", 0)
        latency_ms = latency_us / 1000
        if latency_ms > self.MAX_LATENCY_MS:
            logger.debug(
                f"[TK] Rejeitado por latência: {latency_ms:.1f}ms > {self.MAX_LATENCY_MS}ms"
            )
            return

        # Filtro 2: profit mínimo mais alto
        net_pct = data.get("net_pct", 0)
        if net_pct < self.MIN_PROFIT_PCT:
            return

        # Filtro 3: confluence score
        score = data.get("confluence_score", 0)
        if score < self.MIN_CONFLUENCE_SCORE:
            return

        # Passou nos filtros → executar via base
        await super()._on_opportunity(data)

    async def start(self) -> None:
        await super().start()
        logger.info(
            f"🇯🇵 Tokyo Executor ativo | "
            f"Max latência: {self.MAX_LATENCY_MS}ms | "
            f"Min profit: {self.MIN_PROFIT_PCT}% | "
            f"Min score: {self.MIN_CONFLUENCE_SCORE}"
        )
