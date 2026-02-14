#!/usr/bin/env python3
"""
Sandbox Runner - Executes trading strategies in isolated Docker container

Security:
- Read-only filesystem (except /tmp)
- Network whitelist (Coinbase API only)
- Memory cap (512MB)
- CPU limit (0.5 cores)
- No access to host secrets

Communication:
- Receives strategy via stdin or env var
- Sends signals via stdout (JSON)
- Logs to stderr
"""

import sys
import os
import json
import logging
import traceback
from datetime import datetime
import importlib.util

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stderr  # Logs to stderr, signals to stdout
)
logger = logging.getLogger('sandbox_runner')


class SandboxRunner:
    """
    Runs a trading strategy in isolated sandbox
    """

    def __init__(self, strategy_id=None, strategy_code=None):
        self.strategy_id = strategy_id or os.getenv('STRATEGY_ID')
        self.strategy_code = strategy_code or os.getenv('STRATEGY_CODE')
        self.pair = os.getenv('TRADING_PAIR', 'BTC-USD')

        self.strategy = None
        self.signal_count = 0
        self.error_count = 0

        logger.info(f'Sandbox runner initialized: {self.strategy_id}')

    def load_strategy(self):
        """Load strategy from file or code string"""

        if self.strategy_code:
            # Load from code string (passed as env var)
            logger.info('Loading strategy from code string')

            try:
                # Write to temp file
                temp_file = f'/tmp/strategy_{self.strategy_id}.py'
                with open(temp_file, 'w') as f:
                    f.write(self.strategy_code)

                # Import module
                spec = importlib.util.spec_from_file_location('strategy_module', temp_file)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                # Instantiate strategy class
                self.strategy = module.Strategy(self.pair)

                logger.info(f'Strategy loaded successfully')
                return True

            except Exception as e:
                logger.error(f'Failed to load strategy: {e}')
                logger.error(traceback.format_exc())
                return False

        elif self.strategy_id:
            # Load from generated strategies directory
            strategy_file = f'/app/agents/generated_strategies/{self.strategy_id}.py'

            if not os.path.exists(strategy_file):
                logger.error(f'Strategy file not found: {strategy_file}')
                return False

            try:
                spec = importlib.util.spec_from_file_location('strategy_module', strategy_file)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                self.strategy = module.Strategy(self.pair)

                logger.info(f'Strategy {self.strategy_id} loaded')
                return True

            except Exception as e:
                logger.error(f'Failed to load strategy: {e}')
                logger.error(traceback.format_exc())
                return False

        else:
            logger.error('No strategy_id or strategy_code provided')
            return False

    def run_signal_generation(self, candle_data):
        """
        Generate signal from strategy

        Args:
            candle_data: List of candles (from stdin or API)

        Returns:
            dict with signal
        """

        if not self.strategy:
            return {
                'error': 'strategy_not_loaded',
                'signal': 'HOLD',
                'confidence': 0.0
            }

        try:
            signal = self.strategy.generate_signal(candle_data)

            self.signal_count += 1

            # Validate signal format
            if not isinstance(signal, dict):
                raise ValueError('Signal must be a dict')

            if 'signal' not in signal or signal['signal'] not in ['BUY', 'SELL', 'HOLD']:
                raise ValueError('Signal must have valid "signal" field')

            if 'confidence' not in signal or not (0 <= signal['confidence'] <= 1):
                raise ValueError('Signal must have confidence 0-1')

            # Add metadata
            signal['strategy_id'] = self.strategy_id
            signal['pair'] = self.pair
            signal['timestamp'] = datetime.utcnow().isoformat()
            signal['sandbox'] = True

            return signal

        except Exception as e:
            self.error_count += 1
            logger.error(f'Signal generation error: {e}')
            logger.error(traceback.format_exc())

            return {
                'error': str(e),
                'signal': 'HOLD',
                'confidence': 0.0,
                'strategy_id': self.strategy_id
            }

    def run_loop(self):
        """
        Main loop: read market data from stdin, output signals to stdout
        """

        logger.info('Starting sandbox runner loop')

        while True:
            try:
                # Read market data from stdin (JSON lines)
                line = sys.stdin.readline()

                if not line:
                    break

                data = json.loads(line.strip())

                # Generate signal
                signal = self.run_signal_generation(data.get('candles', []))

                # Output signal to stdout (JSON line)
                print(json.dumps(signal), flush=True)

            except KeyboardInterrupt:
                logger.info('Shutting down...')
                break

            except Exception as e:
                logger.error(f'Loop error: {e}')
                logger.error(traceback.format_exc())

                # Output error signal
                error_signal = {
                    'error': str(e),
                    'signal': 'HOLD',
                    'confidence': 0.0,
                    'strategy_id': self.strategy_id
                }
                print(json.dumps(error_signal), flush=True)

        logger.info(f'Sandbox runner exiting: {self.signal_count} signals, {self.error_count} errors')

    def run_single(self, candle_data):
        """
        Run single signal generation (for testing)

        Args:
            candle_data: List of candles

        Returns:
            Signal dict
        """

        signal = self.run_signal_generation(candle_data)
        print(json.dumps(signal), flush=True)
        return signal


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Sandbox Runner')
    parser.add_argument('--strategy-id', help='Strategy ID to run')
    parser.add_argument('--pair', default='BTC-USD', help='Trading pair')
    parser.add_argument('--mode', choices=['loop', 'single'], default='loop',
                        help='Run mode: loop (stdin) or single (test)')

    args = parser.parse_args()

    # Override with args
    if args.strategy_id:
        os.environ['STRATEGY_ID'] = args.strategy_id
    if args.pair:
        os.environ['TRADING_PAIR'] = args.pair

    runner = SandboxRunner()

    if not runner.load_strategy():
        logger.error('Failed to load strategy, exiting')
        sys.exit(1)

    if args.mode == 'loop':
        runner.run_loop()
    else:
        # Single test mode
        test_candles = [
            {'close': 96000 + i*50, 'volume': 1000000, 'timestamp': datetime.utcnow().isoformat()}
            for i in range(50)
        ]
        runner.run_single(test_candles)
