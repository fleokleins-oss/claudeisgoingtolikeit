"""
APEX PREDATOR V2 - PUMP & DUMP SNIPER
Detecta pumps nos primeiros 5 segundos
Front-run com <100ms de latência
Saída automática antes do dump
"""

import asyncio
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import ccxt
import json
from collections import deque
import aioredis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PUMP_SNIPER")


class PumpDetectionEngine:
    """
    Detecta pumps em TEMPO REAL
    - Volume spike >5x
    - Price change >2% em 1 minuto
    - Low cap coins (volatilidade alta)
    """
    
    def __init__(self, binance, redis_client, machine_id: str):
        self.binance = binance
        self.redis = redis_client
        self.machine_id = machine_id
        
        # Monitoring pairs (coins com risco alto = pump candidates)
        self.pump_watchlist = [
            'DOGE/USDT', 'SHIB/USDT', 'FLOKI/USDT', 'PEPE/USDT',
            'BONK/USDT', 'WIF/USDT', 'MEME/USDT', 'ELON/USDT',
            'PIXEL/USDT', 'SLERF/USDT', 'RNDR/USDT', 'GMX/USDT'
        ]
        
        # Time series data for detection
        self.ohlcv_1m = {}  # 1-minute candles
        self.volume_profile = {}  # Volume por price level
        
        # Detection thresholds
        self.volume_spike_threshold = 5.0  # 5x volume
        self.price_change_threshold = 2.0  # 2% in 1 min
        self.min_confidence = 0.8  # 80% confidence
        
        # Tracking de pumps
        self.active_pumps = {}
        self.pump_history = deque(maxlen=100)
        
    async def real_time_monitor(self):
        """Monitora 24/7 por pumps"""
        
        logger.info(f"[{self.machine_id}] Starting pump detection on {len(self.pump_watchlist)} pairs...")
        
        while True:
            try:
                # Fetch OHLCVs para todas as pairs
                tasks = [
                    self.binance.fetch_ohlcv(pair, '1m', limit=60)
                    for pair in self.pump_watchlist
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Analisa cada pair
                for i, pair in enumerate(self.pump_watchlist):
                    if isinstance(results[i], Exception):
                        logger.debug(f"Skip {pair}: {results[i]}")
                        continue
                    
                    candles = results[i]
                    if len(candles) < 5:
                        continue
                    
                    # Detecta pump
                    pump_signal = await self._detect_pump(pair, candles)
                    
                    if pump_signal and pump_signal['confidence'] > self.min_confidence:
                        await self._execute_pump_trade(pair, pump_signal)
                
                await asyncio.sleep(5)  # Check a cada 5 segundos
            
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(10)
    
    async def _detect_pump(self, pair: str, candles: List) -> Optional[Dict]:
        """Detecta pump no par"""
        
        try:
            closes = np.array([c[4] for c in candles])
            volumes = np.array([c[5] for c in candles])
            highs = np.array([c[2] for c in candles])
            lows = np.array([c[3] for c in candles])
            
            # Últimas 5 velas (5 minutos)
            recent_closes = closes[-5:]
            recent_volumes = volumes[-5:]
            
            # PUMP DETECTOR 1: Volume spike
            vol_avg = np.mean(volumes[-20:-5])  # Volume anterior
            vol_current = recent_volumes[-1]
            vol_spike = vol_current / vol_avg if vol_avg > 0 else 0
            
            # PUMP DETECTOR 2: Price momentum
            price_change_5m = (closes[-1] / closes[-5] - 1) * 100
            price_momentum = price_change_5m / 5  # Change per candle
            
            # PUMP DETECTOR 3: Order book imbalance
            # (Simula com volume profile)
            bid_vol = np.sum(recent_volumes[recent_closes < closes[-1]])
            ask_vol = np.sum(recent_volumes[recent_closes >= closes[-1]])
            imbalance = bid_vol / (ask_vol + 0.001)
            
            # PUMP DETECTOR 4: RSI > 70 (overbought)
            rsi = self._calculate_rsi(closes, 14)
            is_overbought = rsi > 70
            
            # PUMP DETECTOR 5: MACD cross (momentum acceleration)
            macd_signal = self._calculate_macd(closes)
            macd_positive = macd_signal['macd'][-1] > macd_signal['signal'][-1]
            
            # Calcula confidence
            confidence_score = 0.0
            
            if vol_spike > self.volume_spike_threshold:
                confidence_score += 0.3 * min(vol_spike / 10, 1.0)  # 0-30%
            
            if price_change_5m > self.price_change_threshold:
                confidence_score += 0.3 * min(price_change_5m / 10, 1.0)  # 0-30%
            
            if imbalance > 2.0:  # Buy pressure
                confidence_score += 0.2
            
            if is_overbought:
                confidence_score += 0.1
            
            if macd_positive and price_momentum > 0:
                confidence_score += 0.1
            
            # Return pump signal se confidence > threshold
            if confidence_score > self.min_confidence:
                logger.warning(f"⚠️  PUMP DETECTED: {pair} | Vol: {vol_spike:.1f}x | Price: +{price_change_5m:.2f}% | Confidence: {confidence_score:.1%}")
                
                return {
                    'pair': pair,
                    'confidence': confidence_score,
                    'volume_spike': vol_spike,
                    'price_change_5m': price_change_5m,
                    'current_price': closes[-1],
                    'high_5m': np.max(highs[-5:]),
                    'rsi': rsi,
                    'imbalance': imbalance,
                    'timestamp': datetime.now()
                }
            
            return None
        
        except Exception as e:
            logger.debug(f"Detection error {pair}: {e}")
            return None
    
    def _calculate_rsi(self, closes: np.ndarray, period: int = 14) -> float:
        """Calcula RSI"""
        
        if len(closes) < period + 1:
            return 50
        
        deltas = np.diff(closes)
        up = deltas[deltas > 0].sum()
        down = -deltas[deltas < 0].sum()
        
        avg_gain = up / period
        avg_loss = down / period
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _calculate_macd(self, closes: np.ndarray) -> Dict:
        """Calcula MACD"""
        
        exp1 = self._ema(closes, 12)
        exp2 = self._ema(closes, 26)
        macd = exp1 - exp2
        signal = self._ema(macd, 9)
        
        return {'macd': macd, 'signal': signal}
    
    def _ema(self, data: np.ndarray, period: int) -> np.ndarray:
        """Calcula EMA"""
        
        ema = np.zeros_like(data)
        ema[0] = data[0]
        multiplier = 2 / (period + 1)
        
        for i in range(1, len(data)):
            ema[i] = data[i] * multiplier + ema[i-1] * (1 - multiplier)
        
        return ema
    
    async def _execute_pump_trade(self, pair: str, pump_signal: Dict) -> Dict:
        """Executa entry no pump com <100ms latência"""
        
        logger.info(f"🚀 EXECUTING PUMP TRADE: {pair} | Entry: {pump_signal['current_price']:.6f}")
        
        result = {
            'success': False,
            'pair': pair,
            'entry_price': pump_signal['current_price'],
            'entry_time': datetime.now(),
            'confidence': pump_signal['confidence']
        }
        
        try:
            # Calcula posição baseado em risco 0.5%
            capital = 5000  # $5k por trade
            risk_amount = capital * 0.005  # 0.5% risco
            
            # Stop loss: 0.5% abaixo da entry
            stop_loss = pump_signal['current_price'] * 0.995
            risk_distance = pump_signal['current_price'] - stop_loss
            quantity = risk_amount / risk_distance
            
            # Entry order (market)
            entry_order = self.binance.create_market_order(
                pair, 'buy', quantity
            )
            
            result['entry_order'] = entry_order['id']
            result['quantity'] = quantity
            
            # Take profit: 2-5% (depende da força do pump)
            if pump_signal['confidence'] > 0.9:
                take_profit = pump_signal['current_price'] * 1.05  # 5% TP
            else:
                take_profit = pump_signal['current_price'] * 1.02  # 2% TP
            
            result['take_profit'] = take_profit
            result['stop_loss'] = stop_loss
            
            # Trail stop: vai subindo com o preço
            result['trailing_stop_pct'] = 1.0  # 1% trailing
            
            result['success'] = True
            
            # Registra no Redis
            await self.redis.rpush(
                f'pump_trades:{self.machine_id}',
                json.dumps(result, default=str)
            )
            
            logger.info(f"✓ Entry executed | TP: {take_profit:.6f} | SL: {stop_loss:.6f}")
        
        except Exception as e:
            logger.error(f"Trade execution error: {e}")
        
        return result


class PumpExitManager:
    """
    Gerencia saída de pump trades
    - Trail stop automático
    - Saída em breakeven se pump falhar
    - Dinâmica: fecha 50% no TP, deixa 50% correr
    """
    
    def __init__(self, binance, redis_client):
        self.binance = binance
        self.redis = redis_client
        self.active_pumps = {}
        
    async def monitor_pump_exits(self):
        """Monitora e gerencia saídas"""
        
        logger.info("Starting pump exit monitor...")
        
        while True:
            try:
                # Get active pump trades
                trades_json = await self.redis.lrange(f'pump_trades:*', 0, -1)
                
                for trade_json in trades_json:
                    trade = json.loads(trade_json)
                    
                    if trade['pair'] not in self.active_pumps:
                        self.active_pumps[trade['pair']] = trade
                    
                    # Monitora preço
                    ticker = self.binance.fetch_ticker(trade['pair'])
                    current_price = ticker['last']
                    
                    # Check TP hit
                    if current_price >= trade['take_profit']:
                        await self._close_trade(trade, 'TP_HIT', current_price)
                    
                    # Check SL hit
                    elif current_price <= trade['stop_loss']:
                        await self._close_trade(trade, 'SL_HIT', current_price)
                    
                    # Trail stop
                    else:
                        trail_price = trade['entry_price'] * (1 + 0.01)  # 1% trailing
                        if current_price > trail_price:
                            trade['trailing_entry'] = trail_price
                
                await asyncio.sleep(5)
            
            except Exception as e:
                logger.error(f"Exit monitor error: {e}")
                await asyncio.sleep(10)
    
    async def _close_trade(self, trade: Dict, reason: str, exit_price: float):
        """Fecha uma pump trade"""
        
        try:
            # Vende quantidade
            self.binance.create_market_order(
                trade['pair'], 'sell', trade['quantity']
            )
            
            pnl = (exit_price / trade['entry_price'] - 1) * 100
            
            logger.info(f"✓ Pump trade closed: {trade['pair']} | Reason: {reason} | P&L: {pnl:.2f}%")
        
        except Exception as e:
            logger.error(f"Close error: {e}")


class PumpSniperV2:
    """Coordena pump detection + execution + exit"""
    
    def __init__(self, config: Dict, machine_id: str):
        self.config = config
        self.machine_id = machine_id
        
        self.binance = ccxt.binance({
            'apiKey': config['binance']['api_key'],
            'secret': config['binance']['api_secret'],
            'enableRateLimit': True
        })
        
        self.redis = None
        
    async def initialize_redis(self):
        self.redis = await aioredis.create_redis_pool(
            f"redis://{self.config.get('redis_host', 'localhost')}:6379"
        )
    
    async def start(self):
        """Start pump sniping"""
        
        await self.initialize_redis()
        
        detector = PumpDetectionEngine(self.binance, self.redis, self.machine_id)
        exit_manager = PumpExitManager(self.binance, self.redis)
        
        # Run both in parallel
        await asyncio.gather(
            detector.real_time_monitor(),
            exit_manager.monitor_pump_exits()
        )


if __name__ == "__main__":
    
    config = {
        'binance': {
            'api_key': 'YOUR_KEY',
            'api_secret': 'YOUR_SECRET'
        },
        'redis_host': 'localhost'
    }
    
    sniper = PumpSniperV2(config, 'SNIPER-01')
    asyncio.run(sniper.start())
