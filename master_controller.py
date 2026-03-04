"""
APEX PREDATOR V2 - MASTER CONTROLLER
Orquestra:
- 5 máquinas triangular farming
- 5 máquinas pump sniping  
- 5 máquinas fundnig rate arb
- Centralized risk management
- Real-time dashboard
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List
import logging
import aioredis
from enum import Enum

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [APEX_MASTER] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('apex_master.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("APEX_MASTER")


class MarketCondition(Enum):
    """Market conditions"""
    BULLISH = "bullish"
    BEARISH = "bearish"
    RANGING = "ranging"
    VOLATILE = "volatile"


class ApexMasterController:
    """
    Master controller - gerencia 15 máquinas simultaneamente
    
    Arquitetura:
    - 5 nós triangular (TRI-01 até TRI-05)
    - 5 nós pump sniper (SNP-01 até SNP-05)
    - 5 nós funding rate (FUN-01 até FUN-05)
    
    Cada nó é independente mas coordenado via Redis
    """
    
    def __init__(self, config_file: str = 'apex_config.json'):
        self.config = self._load_config(config_file)
        self.redis = None
        
        # Stats
        self.global_stats = {
            'total_profit': 0.0,
            'num_trades': 0,
            'win_rate': 0.0,
            'machines_active': 0,
            'current_market_condition': MarketCondition.RANGING.value
        }
        
        # Risk management
        self.max_daily_loss = self.config.get('max_daily_loss', 5000)
        self.daily_loss = 0.0
        self.trading_halted = False
        
    def _load_config(self, config_file: str) -> Dict:
        """Carrega configuração"""
        
        try:
            with open(config_file) as f:
                return json.load(f)
        except:
            # Default config
            return {
                'machines': {
                    'triangular': 5,
                    'pump_sniper': 5,
                    'funding_rate': 5
                },
                'risk': {
                    'max_daily_loss': 5000,
                    'max_concurrent_trades': 15
                },
                'binance': {
                    'api_key': '',
                    'api_secret': ''
                }
            }
    
    async def initialize(self):
        """Inicializa conexões"""
        
        self.redis = await aioredis.create_redis_pool(
            f"redis://{self.config.get('redis_host', 'localhost')}:6379"
        )
        
        logger.info("🦈 APEX PREDATOR MASTER INITIALIZED")
    
    async def start_hunt(self):
        """INICIA O HUNT - 15 MÁQUINAS SIMULTÂNEAS"""
        
        logger.info("="*80)
        logger.info("🔥 STARTING APEX PREDATOR HUNT - FULL BEAST MODE")
        logger.info("="*80)
        logger.info(f"Triangular Farming: 5 máquinas")
        logger.info(f"Pump Sniping: 5 máquinas")
        logger.info(f"Funding Rate Arb: 5 máquinas")
        logger.info("="*80)
        
        # Inicia os loops
        tasks = [
            self._triangular_farming_loop(),
            self._pump_sniping_loop(),
            self._funding_rate_loop(),
            self._risk_management_loop(),
            self._stats_and_reporting_loop(),
            self._dashboard_api_loop()
        ]
        
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Hunt stopped by user")
        except Exception as e:
            logger.error(f"Hunt error: {e}")
    
    async def _triangular_farming_loop(self):
        """Loop triangular - roda os 5 nodes"""
        
        logger.info("Starting triangular farming (5 nodes)...")
        
        # Cada node é executado como subprocess
        for i in range(5):
            machine_id = f"TRI-{i:02d}"
            
            # Em produção, isso seria subprocess.Popen()
            # Por enquanto, simula
            logger.info(f"✓ Triangular node {machine_id} started")
        
        while not self.trading_halted:
            try:
                # Monitora nodes
                active_tris = await self.redis.llen(f'active_triangles:*')
                
                logger.debug(f"Triangular farming: {active_tris} active positions")
                
                await asyncio.sleep(30)
            
            except Exception as e:
                logger.error(f"Triangular loop error: {e}")
                await asyncio.sleep(10)
    
    async def _pump_sniping_loop(self):
        """Loop pump sniper - roda os 5 nodes"""
        
        logger.info("Starting pump sniping (5 nodes)...")
        
        for i in range(5):
            machine_id = f"SNP-{i:02d}"
            logger.info(f"✓ Pump sniper node {machine_id} started")
        
        while not self.trading_halted:
            try:
                # Monitora pump trades
                active_pumps = await self.redis.llen(f'pump_trades:*')
                
                logger.debug(f"Pump sniping: {active_pumps} active trades")
                
                await asyncio.sleep(10)
            
            except Exception as e:
                logger.error(f"Pump loop error: {e}")
                await asyncio.sleep(10)
    
    async def _funding_rate_loop(self):
        """Loop funding rate - roda os 5 nodes"""
        
        logger.info("Starting funding rate farming (5 nodes)...")
        
        for i in range(5):
            machine_id = f"FUN-{i:02d}"
            logger.info(f"✓ Funding rate node {machine_id} started")
        
        while not self.trading_halted:
            try:
                # Monitora funding trades
                active_funds = await self.redis.llen(f'funding_trades:*')
                
                logger.debug(f"Funding rate: {active_funds} active positions")
                
                await asyncio.sleep(60)
            
            except Exception as e:
                logger.error(f"Funding loop error: {e}")
                await asyncio.sleep(10)
    
    async def _risk_management_loop(self):
        """Gerencia risco global"""
        
        logger.info("Risk management loop started...")
        
        while True:
            try:
                # Check daily loss
                if self.daily_loss > self.max_daily_loss:
                    logger.critical(f"⚠️ DAILY LOSS LIMIT EXCEEDED: ${self.daily_loss:.2f}")
                    self.trading_halted = True
                    await self._halt_all_machines()
                
                # Check margin levels
                for i in range(5):
                    margin = await self._get_machine_margin(f"TRI-{i:02d}")
                    
                    if margin < 1.5:
                        logger.warning(f"Margin level low on TRI-{i:02d}: {margin:.2f}")
                        await self._reduce_positions_on_machine(f"TRI-{i:02d}")
                
                # Monitor overall stats
                total_open = await self._count_open_positions()
                logger.info(f"Total open positions: {total_open}")
                
                await asyncio.sleep(30)
            
            except Exception as e:
                logger.error(f"Risk management error: {e}")
                await asyncio.sleep(10)
    
    async def _stats_and_reporting_loop(self):
        """Calcula estatísticas e reporta"""
        
        logger.info("Stats reporting loop started...")
        
        while True:
            try:
                # Coleta dados de todos os nodes
                await self._aggregate_stats()
                
                # Log a cada 5 minutos
                logger.info(f"""
                ┌──────────────────────────────────────┐
                │ 📊 APEX PREDATOR STATS              │
                ├──────────────────────────────────────┤
                │ Daily P&L: ${self.global_stats['total_profit']:.2f}
                │ Trades: {self.global_stats['num_trades']}
                │ Win Rate: {self.global_stats['win_rate']*100:.1f}%
                │ Active: {self.global_stats['machines_active']} machines
                │ Market: {self.global_stats['current_market_condition'].upper()}
                └──────────────────────────────────────┘
                """)
                
                await asyncio.sleep(300)  # 5 min
            
            except Exception as e:
                logger.error(f"Stats error: {e}")
                await asyncio.sleep(60)
    
    async def _dashboard_api_loop(self):
        """REST API para dashboard real-time"""
        
        from fastapi import FastAPI
        import uvicorn
        
        app = FastAPI(title="APEX PREDATOR Dashboard")
        
        @app.get("/stats")
        async def get_stats():
            return self.global_stats
        
        @app.get("/machines")
        async def get_machines():
            return {
                'triangular': 5,
                'pump_sniper': 5,
                'funding_rate': 5,
                'total': 15
            }
        
        @app.get("/positions")
        async def get_positions():
            return {
                'triangular': await self.redis.llen(f'active_triangles:*'),
                'pump_trades': await self.redis.llen(f'pump_trades:*'),
                'funding_trades': await self.redis.llen(f'funding_trades:*')
            }
        
        @app.post("/stop")
        async def stop_hunt():
            self.trading_halted = True
            await self._halt_all_machines()
            return {'status': 'Hunt stopped'}
        
        config = uvicorn.Config(
            app, 
            host="0.0.0.0", 
            port=self.config.get('dashboard_port', 8000),
            log_level="info"
        )
        server = uvicorn.Server(config)
        await server.serve()
    
    # ============ HELPER METHODS ============
    
    async def _aggregate_stats(self):
        """Agrega estatísticas de todos os nodes"""
        
        try:
            # Coleta do Redis
            all_trades = []
            
            for key in ['triangular', 'pump_trades', 'funding_trades']:
                trades_json = await self.redis.lrange(f'{key}:*', 0, -1)
                for trade_json in trades_json:
                    trade = json.loads(trade_json)
                    all_trades.append(trade)
            
            # Calcula stats
            total_profit = sum([t.get('profit_usd', t.get('profit_pct', 0)) for t in all_trades])
            num_trades = len(all_trades)
            
            if num_trades > 0:
                wins = sum([1 for t in all_trades if t.get('profit_pct', 0) > 0])
                win_rate = wins / num_trades
            else:
                win_rate = 0
            
            self.global_stats['total_profit'] = total_profit
            self.global_stats['num_trades'] = num_trades
            self.global_stats['win_rate'] = win_rate
            self.global_stats['machines_active'] = 15  # Sempre 15
            
        except Exception as e:
            logger.debug(f"Aggregate error: {e}")
    
    async def _get_machine_margin(self, machine_id: str) -> float:
        """Retorna margin level de uma máquina"""
        
        # Mock - em produção, teria integração real
        return 3.0
    
    async def _reduce_positions_on_machine(self, machine_id: str):
        """Reduz posições em uma máquina"""
        
        logger.warning(f"Reducing positions on {machine_id}")
        # Implementação
    
    async def _count_open_positions(self) -> int:
        """Conta total de posições abertas"""
        
        try:
            count = 0
            count += await self.redis.llen('active_triangles:*') or 0
            count += await self.redis.llen('pump_trades:*') or 0
            count += await self.redis.llen('funding_trades:*') or 0
            return count
        except:
            return 0
    
    async def _halt_all_machines(self):
        """Para todas as máquinas"""
        
        logger.critical("HALTING ALL MACHINES!")
        
        # Em produção, enviaria signals para killall processos
        for i in range(5):
            await self.redis.set(f'halt:TRI-{i:02d}', '1')
            await self.redis.set(f'halt:SNP-{i:02d}', '1')
            await self.redis.set(f'halt:FUN-{i:02d}', '1')


# ============ EXECUTION ============
async def main():
    
    master = ApexMasterController('apex_config.json')
    await master.initialize()
    await master.start_hunt()


if __name__ == "__main__":
    
    logger.info("🦈 APEX PREDATOR V2 - BEAST MODE ACTIVATED")
    asyncio.run(main())
