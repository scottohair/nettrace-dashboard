#!/usr/bin/env python3
"""
Multi-Exchange Arbitrage Service - Runs continuously on Fly.io
Scans for arbitrage opportunities across 10 exchanges and executes profitable trades
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from multi_exchange_orchestrator import MultiExchangeOrchestrator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('arb_service')

class ArbitrageService:
    """Continuous arbitrage scanning service"""

    def __init__(self):
        self.orchestrator = MultiExchangeOrchestrator()
        self.running = False
        self.scan_interval = 60  # seconds between scans

    async def start(self):
        """Start the continuous arbitrage service"""
        self.running = True
        logger.info('🚀 Multi-Exchange Arbitrage Service starting...')

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        scan_count = 0
        total_opportunities = 0

        while self.running:
            try:
                scan_count += 1
                logger.info(f'Starting scan #{scan_count}...')

                # Run arbitrage scan
                opportunities = await self.orchestrator.run_scan()
                total_opportunities += len(opportunities)

                if opportunities:
                    logger.info(f'✅ Found {len(opportunities)} arbitrage opportunities!')
                    for opp in opportunities:
                        logger.info(
                            f"  {opp['pair']}: {opp['buy_exchange']} ${opp['buy_price']:.2f} → "
                            f"{opp['sell_exchange']} ${opp['sell_price']:.2f} "
                            f"(spread: {opp['spread_pct']:.2f}%, profit: ${opp['profit_usd']:.2f})"
                        )

                        # TODO: Execute arbitrage trade via exchange_connector
                        # For now, just log the opportunity

                else:
                    logger.info(f'No arbitrage opportunities found in scan #{scan_count}')

                # Stats
                if scan_count % 10 == 0:
                    logger.info(
                        f'📊 Stats: {scan_count} scans completed, '
                        f'{total_opportunities} total opportunities found '
                        f'({total_opportunities/scan_count:.1f} avg/scan)'
                    )

                # Wait before next scan
                logger.info(f'Waiting {self.scan_interval}s before next scan...')
                await asyncio.sleep(self.scan_interval)

            except Exception as e:
                logger.error(f'Error in arbitrage scan: {e}', exc_info=True)
                # Continue running even if one scan fails
                await asyncio.sleep(self.scan_interval)

        logger.info('Arbitrage service stopped')

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f'Received signal {signum}, shutting down gracefully...')
        self.running = False

async def main():
    """Main entry point"""
    service = ArbitrageService()
    await service.start()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Service interrupted by user')
    except Exception as e:
        logger.error(f'Fatal error: {e}', exc_info=True)
        sys.exit(1)
