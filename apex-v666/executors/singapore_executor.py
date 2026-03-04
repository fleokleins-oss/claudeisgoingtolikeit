"""
executors/singapore_executor.py
APEX PREDATOR NEO v666 – Executor Singapore

Especialização (executor primário, mais agressivo):
 - Latência máxima aceita: 60ms (rejeita se pubsub demorar mais)
 - Confluence score mínimo: 70 (mais exigente que padrão)
 - Prioriza velocidade sobre profit — pega oportunidades rápido
"""
from __future__ import annotations

from typing import Any, Dict

from loguru import logger

from executors.base_executor import BaseExecutor


class SingaporeExecutor(BaseExecutor):
    """Executor primário — baixa latência, alto critério de confluência."""

    MAX_LATENCY_MS: float = 60.0
    MIN_CONFLUENCE_SCORE: float = 70.0

    def __init__(self) -> None:
        super().__init__(region="singapore")

    async def _on_opportunity(self, data: Dict[str, Any]) -> None:
        """Override: filtro de latência + score antes de executar."""
        # Filtro 1: latência da mensagem Redis
        latency_us = data.get("_latency_us", 0)
        latency_ms = latency_us / 1000
        if latency_ms > self.MAX_LATENCY_MS:
            logger.debug(
                f"[SG] Rejeitado por latência: {latency_ms:.1f}ms > {self.MAX_LATENCY_MS}ms"
            )
            return

        # Filtro 2: confluence score mínimo
        score = data.get("confluence_score", 0)
        if score < self.MIN_CONFLUENCE_SCORE:
            return

        # Passou nos filtros → executar via base
        await super()._on_opportunity(data)

    async def start(self) -> None:
        await super().start()
        logger.info(
            f"🇸🇬 Singapore Executor ativo | "
            f"Max latência: {self.MAX_LATENCY_MS}ms | "
            f"Min score: {self.MIN_CONFLUENCE_SCORE}"
        )
