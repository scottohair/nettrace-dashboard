#!/usr/bin/env python3
"""
Quick portfolio checker - connects to Coinbase API and shows current holdings
"""

import asyncio
import os
import sys
from pathlib import Path

# Add agents to path
sys.path.insert(0, str(Path(__file__).parent / 'agents'))

# Load environment
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / 'agents' / '.env')

# Now import after env is loaded
import aiohttp
import jwt
import time
from cryptography.hazmat.primitives import serialization

API_KEY_ID = os.getenv('COINBASE_API_KEY_ID')
API_KEY_SECRET = os.getenv('COINBASE_API_KEY_SECRET')

def generate_jwt(request_method, request_path):
    """Generate JWT for Coinbase CDP API"""
    key_secret = API_KEY_SECRET.replace('\\n', '\n')

    private_key = serialization.load_pem_private_key(
        key_secret.encode('utf-8'),
        password=None
    )

    uri = f"{request_method} api.coinbase.com{request_path}"

    payload = {
        'sub': API_KEY_ID,
        'iss': 'cdp',
        'nbf': int(time.time()),
        'exp': int(time.time()) + 120,
        'uri': uri
    }

    token = jwt.encode(payload, private_key, algorithm='ES256', headers={'kid': API_KEY_ID, 'nonce': os.urandom(16).hex()})

    return token

async def get_accounts():
    """Get all Coinbase accounts"""
    request_path = '/api/v3/brokerage/accounts'
    token = generate_jwt('GET', request_path)

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    url = f'https://api.coinbase.com{request_path}'

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get('accounts', [])
            else:
                text = await resp.text()
                print(f"Error: {resp.status} - {text}")
                return []

async def get_product_price(product_id):
    """Get current price for a product"""
    request_path = f'/api/v3/brokerage/products/{product_id}'
    token = generate_jwt('GET', request_path)

    headers = {
        'Authorization': f'Bearer {token}'
    }

    url = f'https://api.coinbase.com{request_path}'

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                return float(data.get('price', 0))
            return 0

async def main():
    print('=' * 70)
    print('COINBASE PORTFOLIO CHECK')
    print('=' * 70)
    print()

    accounts = await get_accounts()

    if not accounts:
        print('No accounts found or API error')
        return

    holdings = []
    total_usd = 0

    for acct in accounts:
        currency = acct.get('currency', '')
        available = float(acct.get('available_balance', {}).get('value', 0))

        if available > 0.001:
            # Get current price
            if currency == 'USD' or currency == 'USDC':
                price = 1.0
            else:
                pair = f'{currency}-USD'
                price = await get_product_price(pair)
                if price == 0:
                    pair = f'{currency}-USDC'
                    price = await get_product_price(pair)

            value_usd = available * price
            total_usd += value_usd

            holdings.append({
                'currency': currency,
                'amount': available,
                'price': price,
                'value': value_usd
            })

    # Sort by value
    holdings.sort(key=lambda x: x['value'], reverse=True)

    print(f'Total Portfolio Value: ${total_usd:.2f}')
    print(f'Number of Holdings: {len(holdings)}')
    print()
    print(f"{'Currency':<10} {'Amount':>15} {'Price':>12} {'Value':>12}")
    print('-' * 70)

    for h in holdings:
        print(f"{h['currency']:<10} {h['amount']:>15.6f} ${h['price']:>11.2f} ${h['value']:>11.2f}")

    print('=' * 70)

    # Check for recent orders
    print()
    print('RECENT ORDERS (last 10):')
    print('=' * 70)

    request_path = '/api/v3/brokerage/orders/historical/fills?limit=10'
    token = generate_jwt('GET', request_path)
    headers = {'Authorization': f'Bearer {token}'}
    url = f'https://api.coinbase.com{request_path}'

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                fills = data.get('fills', [])
                if fills:
                    for fill in fills[:10]:
                        print(f"{fill.get('trade_time', '')[:19]} {fill.get('side', ''):4s} {fill.get('product_id', ''):<12s} "
                              f"{float(fill.get('size', 0)):>10.6f} @ ${float(fill.get('price', 0)):>8.2f} "
                              f"fee=${float(fill.get('commission', 0)):>6.4f}")
                else:
                    print('No recent fills')
            else:
                print(f'Could not fetch fills: {resp.status}')

if __name__ == '__main__':
    asyncio.run(main())
