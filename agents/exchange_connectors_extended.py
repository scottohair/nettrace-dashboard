#!/usr/bin/env python3
"""
Extended Exchange Connectors - EPOCH 3
Additional connectors for Bybit, OKX, Deribit, Gate.io, MEXC, Uniswap

Total: 10 exchanges (Coinbase, Kraken, Binance, Alpaca + 6 new)
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import Dict, Optional
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('exchange_connectors')


class BybitConnector:
    """Bybit derivatives exchange"""

    def __init__(self):
        self.name = 'bybit'
        self.base_url = 'https://api.bybit.com/v5/market'

    async def get_price(self, pair: str) -> Optional[Dict]:
        """Get Bybit price"""
        try:
            # Convert BTC-USD to BTCUSDT
            symbol = pair.replace('-', '').replace('USD', 'USDT')

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f'{self.base_url}/tickers?category=spot&symbol={symbol}'
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if 'result' in data and 'list' in data['result']:
                            ticker = data['result']['list'][0]
                            return {
                                'exchange': self.name,
                                'pair': pair,
                                'price': float(ticker['lastPrice']),
                                'volume_24h': float(ticker['volume24h']),
                                'timestamp': datetime.utcnow().isoformat()
                            }
        except Exception as e:
            logger.error(f'Bybit price fetch failed: {e}')
        return None

    async def get_funding_rate(self, pair: str) -> Optional[float]:
        """Get perpetual funding rate"""
        try:
            symbol = pair.replace('-', '').replace('USD', 'USDT')

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f'{self.base_url}/funding/history?category=linear&symbol={symbol}&limit=1'
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if 'result' in data and 'list' in data['result']:
                            return float(data['result']['list'][0]['fundingRate'])
        except Exception as e:
            logger.error(f'Bybit funding rate fetch failed: {e}')
        return None


class OKXConnector:
    """OKX exchange"""

    def __init__(self):
        self.name = 'okx'
        self.base_url = 'https://www.okx.com/api/v5/market'

    async def get_price(self, pair: str) -> Optional[Dict]:
        """Get OKX price"""
        try:
            # Convert BTC-USD to BTC-USDT
            symbol = pair.replace('USD', 'USDT')

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f'{self.base_url}/ticker?instId={symbol}'
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if 'data' in data and data['data']:
                            ticker = data['data'][0]
                            return {
                                'exchange': self.name,
                                'pair': pair,
                                'price': float(ticker['last']),
                                'volume_24h': float(ticker['vol24h']),
                                'timestamp': datetime.utcnow().isoformat()
                            }
        except Exception as e:
            logger.error(f'OKX price fetch failed: {e}')
        return None


class DeribitConnector:
    """Deribit options/futures exchange"""

    def __init__(self):
        self.name = 'deribit'
        self.base_url = 'https://www.deribit.com/api/v2/public'

    async def get_price(self, pair: str) -> Optional[Dict]:
        """Get Deribit index price"""
        try:
            # Get index price (not perpetual)
            currency = pair.split('-')[0]  # BTC, ETH

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f'{self.base_url}/get_index_price?index_name={currency.lower()}_usd'
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if 'result' in data:
                            return {
                                'exchange': self.name,
                                'pair': pair,
                                'price': float(data['result']['index_price']),
                                'timestamp': datetime.utcnow().isoformat()
                            }
        except Exception as e:
            logger.error(f'Deribit price fetch failed: {e}')
        return None


class GateIOConnector:
    """Gate.io exchange"""

    def __init__(self):
        self.name = 'gateio'
        self.base_url = 'https://api.gateio.ws/api/v4/spot'

    async def get_price(self, pair: str) -> Optional[Dict]:
        """Get Gate.io price"""
        try:
            # Convert BTC-USD to BTC_USDT
            symbol = pair.replace('-', '_').replace('USD', 'USDT')

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f'{self.base_url}/tickers?currency_pair={symbol}'
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data:
                            ticker = data[0]
                            return {
                                'exchange': self.name,
                                'pair': pair,
                                'price': float(ticker['last']),
                                'volume_24h': float(ticker['base_volume']),
                                'timestamp': datetime.utcnow().isoformat()
                            }
        except Exception as e:
            logger.error(f'Gate.io price fetch failed: {e}')
        return None


class MEXCConnector:
    """MEXC exchange"""

    def __init__(self):
        self.name = 'mexc'
        self.base_url = 'https://api.mexc.com/api/v3'

    async def get_price(self, pair: str) -> Optional[Dict]:
        """Get MEXC price"""
        try:
            # Convert BTC-USD to BTCUSDT
            symbol = pair.replace('-', '').replace('USD', 'USDT')

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f'{self.base_url}/ticker/price?symbol={symbol}'
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return {
                            'exchange': self.name,
                            'pair': pair,
                            'price': float(data['price']),
                            'timestamp': datetime.utcnow().isoformat()
                        }
        except Exception as e:
            logger.error(f'MEXC price fetch failed: {e}')
        return None


class UniswapConnector:
    """Uniswap DEX (via The Graph)"""

    def __init__(self):
        self.name = 'uniswap'
        self.graph_url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'

    async def get_price(self, pair: str) -> Optional[Dict]:
        """Get Uniswap price from pools"""
        try:
            # Map to token addresses (hardcoded for major pairs)
            token_addresses = {
                'BTC-USD': {
                    'token0': '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',  # WBTC
                    'token1': '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'   # USDC
                },
                'ETH-USD': {
                    'token0': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',  # WETH
                    'token1': '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'   # USDC
                }
            }

            if pair not in token_addresses:
                return None

            # GraphQL query for pool price
            query = '''
            {
              pools(
                where: {
                  token0: "%s"
                  token1: "%s"
                }
                orderBy: volumeUSD
                orderDirection: desc
                first: 1
              ) {
                token0Price
                token1Price
                volumeUSD
              }
            }
            ''' % (token_addresses[pair]['token0'], token_addresses[pair]['token1'])

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.graph_url,
                    json={'query': query}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if 'data' in data and 'pools' in data['data'] and data['data']['pools']:
                            pool = data['data']['pools'][0]
                            return {
                                'exchange': self.name,
                                'pair': pair,
                                'price': float(pool['token0Price']),
                                'volume_24h': float(pool['volumeUSD']),
                                'timestamp': datetime.utcnow().isoformat()
                            }
        except Exception as e:
            logger.error(f'Uniswap price fetch failed: {e}')
        return None


class ExchangeManager:
    """Manages all exchange connectors"""

    def __init__(self):
        self.exchanges = {
            'bybit': BybitConnector(),
            'okx': OKXConnector(),
            'deribit': DeribitConnector(),
            'gateio': GateIOConnector(),
            'mexc': MEXCConnector(),
            'uniswap': UniswapConnector()
        }

        logger.info(f'Exchange Manager initialized: {len(self.exchanges)} exchanges')

    async def fetch_all_prices(self, pair: str) -> Dict[str, Dict]:
        """Fetch price from all exchanges"""

        tasks = {}
        for name, exchange in self.exchanges.items():
            tasks[name] = exchange.get_price(pair)

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        prices = {}
        for name, result in zip(tasks.keys(), results):
            if not isinstance(result, Exception) and result is not None:
                prices[name] = result

        return prices

    def get_exchange_count(self):
        """Get total exchange count"""
        # 6 new + 4 existing (Coinbase, Kraken, Binance, Alpaca)
        return len(self.exchanges) + 4


async def main():
    print('🌐 Extended Exchange Connectors - EPOCH 3')
    print('='*70)

    manager = ExchangeManager()

    print(f'\n📊 Total exchanges: {manager.get_exchange_count()} (6 new + 4 existing)\n')

    # Test fetching prices
    print('🔍 Testing price fetch for BTC-USD...\n')

    prices = await manager.fetch_all_prices('BTC-USD')

    print(f'📈 Prices from {len(prices)} exchanges:')
    for exchange, price_data in prices.items():
        print(f'   {exchange:10s}: ${price_data["price"]:>10,.2f}')

    if len(prices) >= 2:
        # Calculate spread
        prices_list = [p['price'] for p in prices.values()]
        min_price = min(prices_list)
        max_price = max(prices_list)
        spread = (max_price - min_price) / min_price

        print(f'\n💰 Arbitrage Opportunity:')
        print(f'   Min: ${min_price:,.2f}')
        print(f'   Max: ${max_price:,.2f}')
        print(f'   Spread: {spread*100:.2f}%')

    print('\n✅ Exchange connectors ready')


if __name__ == '__main__':
    asyncio.run(main())
