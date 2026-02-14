#!/usr/bin/env python3
"""
Agent Message Bus - Redis pub/sub for real-time agent coordination

Channels:
- /signals/{pair}         → Trading signals from all agents
- /positions/open         → Position opened notifications
- /positions/close        → Position closed notifications
- /alerts/risk            → Risk alerts (drawdown, exposure)
- /alerts/opportunity     → High-confidence opportunities
- /system/heartbeat       → Agent health checks
- /coordination/{pair}    → Multi-agent coordination

Benefits:
- Real-time coordination between 100+ agents
- No polling (event-driven)
- Decouple agents (add/remove without changes)
- Cross-region communication (ewr ↔ lhr ↔ nrt)
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import json
import logging
import time
from datetime import datetime
from typing import Callable, Dict, Any, Optional
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('message_bus')

# Optional Redis import
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning('redis module not installed (pip install redis) - using mock mode')


class MessageBus:
    """
    Redis pub/sub message bus for agent coordination
    """

    def __init__(self, redis_url=None, mock_mode=None):
        self.redis_url = redis_url or os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.mock_mode = mock_mode if mock_mode is not None else not REDIS_AVAILABLE

        if self.mock_mode:
            logger.warning('Running in MOCK MODE - messages not persisted')
            self.redis = None
            self.pubsub = None
            self.mock_subscribers = {}  # channel -> list of callbacks
            self.mock_messages = []  # For testing
        else:
            try:
                self.redis = redis.from_url(self.redis_url, decode_responses=True)
                self.pubsub = self.redis.pubsub()
                self.redis.ping()
                logger.info(f'Connected to Redis: {self.redis_url}')
            except Exception as e:
                logger.error(f'Failed to connect to Redis: {e}')
                logger.warning('Falling back to MOCK MODE')
                self.mock_mode = True
                self.redis = None
                self.pubsub = None
                self.mock_subscribers = {}
                self.mock_messages = []

        self.subscribers = {}  # channel -> callback
        self.running = False
        self.listener_thread = None

    def publish(self, channel: str, data: Dict[str, Any], agent_id: str = None):
        """
        Publish message to channel

        Args:
            channel: Channel name (e.g., '/signals/btc')
            data: Message data (dict)
            agent_id: Optional agent ID
        """

        message = {
            'channel': channel,
            'agent_id': agent_id or 'unknown',
            'timestamp': datetime.utcnow().isoformat(),
            'data': data
        }

        message_json = json.dumps(message)

        if self.mock_mode:
            # Mock mode: deliver to local subscribers
            self.mock_messages.append(message)

            if channel in self.mock_subscribers:
                for callback in self.mock_subscribers[channel]:
                    try:
                        callback(message)
                    except Exception as e:
                        logger.error(f'Subscriber callback error: {e}')

            logger.debug(f'[MOCK] Published to {channel}: {data}')
        else:
            # Real mode: publish to Redis
            try:
                self.redis.publish(channel, message_json)
                logger.debug(f'Published to {channel}: {data}')
            except Exception as e:
                logger.error(f'Failed to publish to {channel}: {e}')

    def subscribe(self, channel: str, callback: Callable[[Dict[str, Any]], None]):
        """
        Subscribe to channel

        Args:
            channel: Channel name (e.g., '/signals/btc')
            callback: Function to call when message received
        """

        if self.mock_mode:
            # Mock mode: register local callback
            if channel not in self.mock_subscribers:
                self.mock_subscribers[channel] = []
            self.mock_subscribers[channel].append(callback)
            logger.info(f'[MOCK] Subscribed to {channel}')
        else:
            # Real mode: subscribe via Redis
            self.subscribers[channel] = callback
            self.pubsub.subscribe(channel)
            logger.info(f'Subscribed to {channel}')

    def unsubscribe(self, channel: str):
        """Unsubscribe from channel"""

        if self.mock_mode:
            if channel in self.mock_subscribers:
                del self.mock_subscribers[channel]
            logger.info(f'[MOCK] Unsubscribed from {channel}')
        else:
            if channel in self.subscribers:
                del self.subscribers[channel]
            self.pubsub.unsubscribe(channel)
            logger.info(f'Unsubscribed from {channel}')

    def run(self, blocking=True):
        """
        Start listening for messages

        Args:
            blocking: If True, blocks current thread. If False, runs in background thread.
        """

        if self.mock_mode:
            logger.info('[MOCK] Message bus running (mock mode)')
            # In mock mode, messages are delivered immediately on publish
            # No need to run a listener thread
            return

        if blocking:
            self._listen_loop()
        else:
            self.running = True
            self.listener_thread = threading.Thread(target=self._listen_loop, daemon=True)
            self.listener_thread.start()
            logger.info('Message bus running in background')

    def _listen_loop(self):
        """Main message listening loop"""

        self.running = True
        logger.info('Message bus listening...')

        try:
            for message in self.pubsub.listen():
                if not self.running:
                    break

                if message['type'] == 'message':
                    channel = message['channel']
                    data = json.loads(message['data'])

                    if channel in self.subscribers:
                        try:
                            self.subscribers[channel](data)
                        except Exception as e:
                            logger.error(f'Subscriber callback error on {channel}: {e}')

        except KeyboardInterrupt:
            logger.info('Message bus stopping...')
        except Exception as e:
            logger.error(f'Message bus error: {e}')
        finally:
            self.running = False

    def stop(self):
        """Stop message bus"""

        self.running = False

        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join(timeout=5)

        if self.pubsub:
            self.pubsub.close()

        logger.info('Message bus stopped')

    def publish_signal(self, pair: str, signal: str, confidence: float,
                      reason: str, agent_id: str, price: float = None):
        """
        Publish trading signal

        Args:
            pair: Trading pair (e.g., 'BTC-USD')
            signal: BUY/SELL/HOLD
            confidence: 0.0-1.0
            reason: Signal reason
            agent_id: Agent that generated signal
            price: Optional current price
        """

        channel = f'/signals/{pair.lower().replace("-", "")}'

        self.publish(channel, {
            'signal': signal,
            'confidence': confidence,
            'pair': pair,
            'price': price,
            'reason': reason
        }, agent_id=agent_id)

    def publish_position_open(self, pair: str, side: str, size: float,
                             price: float, agent_id: str):
        """Publish position opened notification"""

        self.publish('/positions/open', {
            'pair': pair,
            'side': side,
            'size': size,
            'price': price
        }, agent_id=agent_id)

    def publish_position_close(self, pair: str, pnl: float, reason: str, agent_id: str):
        """Publish position closed notification"""

        self.publish('/positions/close', {
            'pair': pair,
            'pnl': pnl,
            'reason': reason
        }, agent_id=agent_id)

    def publish_risk_alert(self, alert_type: str, severity: str,
                          message: str, data: Dict[str, Any], agent_id: str):
        """
        Publish risk alert

        Args:
            alert_type: e.g., 'drawdown', 'exposure', 'correlation'
            severity: 'low', 'medium', 'high', 'critical'
            message: Human-readable message
            data: Additional context
            agent_id: Agent that detected risk
        """

        self.publish('/alerts/risk', {
            'alert_type': alert_type,
            'severity': severity,
            'message': message,
            'data': data
        }, agent_id=agent_id)

    def publish_opportunity(self, opportunity_type: str, pair: str,
                           confidence: float, expected_return: float,
                           data: Dict[str, Any], agent_id: str):
        """Publish high-confidence opportunity"""

        self.publish('/alerts/opportunity', {
            'opportunity_type': opportunity_type,
            'pair': pair,
            'confidence': confidence,
            'expected_return': expected_return,
            'data': data
        }, agent_id=agent_id)

    def publish_heartbeat(self, agent_id: str, status: str, metrics: Dict[str, Any]):
        """Publish agent heartbeat"""

        self.publish('/system/heartbeat', {
            'status': status,
            'metrics': metrics
        }, agent_id=agent_id)


class BusSubscriber:
    """
    Base class for agents that subscribe to message bus
    """

    def __init__(self, agent_id: str, bus: MessageBus = None):
        self.agent_id = agent_id
        self.bus = bus or MessageBus()
        self.subscriptions = []

    def subscribe_signals(self, pairs: list, callback: Callable):
        """Subscribe to signals for specific pairs"""

        for pair in pairs:
            channel = f'/signals/{pair.lower().replace("-", "")}'
            self.bus.subscribe(channel, callback)
            self.subscriptions.append(channel)

    def subscribe_positions(self, callback: Callable):
        """Subscribe to position notifications"""

        self.bus.subscribe('/positions/open', callback)
        self.bus.subscribe('/positions/close', callback)
        self.subscriptions.extend(['/positions/open', '/positions/close'])

    def subscribe_risk_alerts(self, callback: Callable):
        """Subscribe to risk alerts"""

        self.bus.subscribe('/alerts/risk', callback)
        self.subscriptions.append('/alerts/risk')

    def subscribe_opportunities(self, callback: Callable):
        """Subscribe to opportunity alerts"""

        self.bus.subscribe('/alerts/opportunity', callback)
        self.subscriptions.append('/alerts/opportunity')

    def unsubscribe_all(self):
        """Unsubscribe from all channels"""

        for channel in self.subscriptions:
            self.bus.unsubscribe(channel)
        self.subscriptions = []


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Message Bus Test')
    parser.add_argument('--mode', choices=['publish', 'subscribe', 'test'], default='test')
    parser.add_argument('--channel', default='/signals/btc')
    parser.add_argument('--message', default='test message')

    args = parser.parse_args()

    bus = MessageBus()

    if args.mode == 'publish':
        print(f'Publishing to {args.channel}...')
        bus.publish_signal(
            pair='BTC-USD',
            signal='BUY',
            confidence=0.85,
            reason='test_signal',
            agent_id='test_agent',
            price=96500
        )
        print('✅ Published')

    elif args.mode == 'subscribe':
        print(f'Subscribing to {args.channel}...')

        def handle_message(message):
            print(f'📨 Received: {json.dumps(message, indent=2)}')

        bus.subscribe(args.channel, handle_message)
        print('Listening... (Ctrl+C to stop)')
        bus.run(blocking=True)

    elif args.mode == 'test':
        print('🧪 Running message bus test...\n')

        # Test publisher and subscriber
        messages_received = []

        def handle_signal(message):
            messages_received.append(message)
            print(f'✅ Received signal: {message["data"]["signal"]} for {message["data"]["pair"]} @ {message["data"]["confidence"]:.1%}')

        # Subscribe
        bus.subscribe('/signals/btcusd', handle_signal)
        bus.subscribe('/signals/ethusd', handle_signal)

        # Start listener (non-blocking in mock mode)
        bus.run(blocking=False)

        time.sleep(0.5)

        # Publish signals
        print('📤 Publishing signals...\n')

        bus.publish_signal(
            pair='BTC-USD',
            signal='BUY',
            confidence=0.85,
            reason='momentum_breakout',
            agent_id='sniper',
            price=96500
        )

        bus.publish_signal(
            pair='ETH-USD',
            signal='SELL',
            confidence=0.72,
            reason='resistance_rejection',
            agent_id='exit_manager',
            price=2650
        )

        time.sleep(0.5)

        # Verify
        print(f'\n📊 Test Results:')
        print(f'   Published: 2 signals')
        print(f'   Received: {len(messages_received)} signals')

        if len(messages_received) == 2:
            print('   ✅ ALL TESTS PASSED')
        else:
            print('   ❌ TEST FAILED')

        # Stop
        bus.stop()
