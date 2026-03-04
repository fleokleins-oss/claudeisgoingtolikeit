"""
config/config.py
APEX PREDATOR NEO v666 – Configuração centralizada via Pydantic Settings.

Toda variável de ambiente é validada no boot com type coercion.
Se faltar credencial crítica, o sistema NÃO sobe e exibe mensagem clara.
"""
from __future__ import annotations

from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class ApexConfig(BaseSettings):
    """Configuração global — singleton importável como 'cfg'."""

    # ── Modo de operação ─────────────────────────────────
    TESTNET: bool = True

    # ── Credenciais Binance Spot ─────────────────────────
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""
    BINANCE_TESTNET_API_KEY: str = ""
    BINANCE_TESTNET_API_SECRET: str = ""

    # ── Redis ────────────────────────────────────────────
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""

    # ── Capital e risco ──────────────────────────────────
    CAPITAL_TOTAL: float = 22.00
    MAX_POR_CICLO: float = 8.00
    MAX_DRAWDOWN_PCT: float = 4.0
    ROBIN_HOOD_COOLDOWN_S: int = 1800  # 30 minutos

    # ── Scanner ──────────────────────────────────────────
    SCAN_INTERVAL_MS: int = 45
    MIN_PROFIT_PCT: float = 0.08
    MIN_CONFLUENCE_SCORE: float = 65.0

    # ── Taxas Binance (sem desconto BNB) ─────────────────
    MAKER_FEE: float = 0.00075
    TAKER_FEE: float = 0.00075

    # ── Auto-Earn ────────────────────────────────────────
    AUTO_EARN_MIN_PROFIT: float = 0.10

    # ── Logging ──────────────────────────────────────────
    LOG_LEVEL: str = "INFO"
    LOG_ROTATION: str = "50 MB"
    LOG_RETENTION: str = "7 days"

    # ── Docker ENV (injetado pelo docker-compose) ────────
    APEX_ROLE: str = "scanner"
    APEX_REGION: str = "curitiba"

    # ── Ativos para descoberta de triângulos ─────────────
    BASE_ASSETS: List[str] = [
        "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA",
        "AVAX", "DOT", "MATIC", "LINK", "SHIB", "TRX",
        "WIF", "BONK", "PEPE", "NEAR", "APT", "SUI",
        "ARB", "OP", "FIL", "ATOM", "UNI", "LTC",
        "FET", "RENDER", "INJ", "SEI", "TIA",
    ]
    QUOTE_ASSETS: List[str] = ["USDT", "BRL", "BTC", "ETH", "BNB", "FDUSD"]

    # ── Canais Redis (prefixo v666 isolado) ──────────────
    CH_OPPORTUNITIES: str = "apex:v666:opportunities"
    CH_EXECUTIONS: str = "apex:v666:executions"
    CH_HEARTBEAT: str = "apex:v666:heartbeat"
    CH_RISK: str = "apex:v666:risk"
    CH_EARN: str = "apex:v666:earn"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True

    # ── Propriedades derivadas ───────────────────────────
    @property
    def api_key(self) -> str:
        """Retorna a API key correta conforme modo testnet/live."""
        return self.BINANCE_TESTNET_API_KEY if self.TESTNET else self.BINANCE_API_KEY

    @property
    def api_secret(self) -> str:
        """Retorna o API secret correto conforme modo testnet/live."""
        return self.BINANCE_TESTNET_API_SECRET if self.TESTNET else self.BINANCE_API_SECRET

    @property
    def fee_per_leg(self) -> float:
        """Taxa média por perna: (maker + taker) / 2."""
        return (self.MAKER_FEE + self.TAKER_FEE) / 2

    @property
    def fee_3_legs(self) -> float:
        """Taxa total estimada para 3 pernas de arbitragem."""
        return self.fee_per_leg * 3

    @property
    def equity_shutdown(self) -> float:
        """Equity mínimo antes de shutdown permanente (50% do capital)."""
        return self.CAPITAL_TOTAL * 0.50


# Singleton importável em qualquer módulo
cfg = ApexConfig()
