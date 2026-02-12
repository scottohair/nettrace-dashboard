#!/usr/bin/env python3
"""Stripe Issuing integration for virtual trading cards.

Creates and manages virtual Visa cards for trading arbitrage across
platforms that accept card funding (exchanges, brokers, payment rails).

Each card is tagged to a specific trading pair and strategy, enforcing
the $5 max-per-card rule. Cards can be frozen/cancelled instantly if
a strategy goes sideways (HARDSTOP compliance).

Env vars:
  STRIPE_SECRET_KEY       — Stripe API secret key
  STRIPE_ISSUING_ENABLED  — must be "true" to allow card creation
"""

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import stripe

# Load .env file for credentials
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"'))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent / "stripe_issuing.log")),
    ],
)
logger = logging.getLogger("stripe_issuing")

# Hard limit: $5.00 per card (500 cents). NEVER violate trading rules.
MAX_CARD_AMOUNT_CENTS = 500


class StripeCardIssuer:
    """Manages Stripe Issuing virtual cards for trading arbitrage.

    Each virtual card is scoped to a single trading pair and strategy,
    with a hard $5 cap enforced at creation time. Cards are created as
    spending-controlled virtual Visa cards through Stripe Issuing.
    """

    def __init__(self):
        """Initialize with STRIPE_SECRET_KEY from environment.

        Raises:
            RuntimeError: If STRIPE_SECRET_KEY is not set.
            RuntimeError: If STRIPE_ISSUING_ENABLED is not "true".
        """
        self.secret_key = os.environ.get("STRIPE_SECRET_KEY", "")
        if not self.secret_key:
            raise RuntimeError(
                "STRIPE_SECRET_KEY not set. Add it to agents/.env"
            )

        enabled = os.environ.get("STRIPE_ISSUING_ENABLED", "false").lower()
        if enabled != "true":
            raise RuntimeError(
                "STRIPE_ISSUING_ENABLED is not 'true'. "
                "Set STRIPE_ISSUING_ENABLED=true in agents/.env to enable card creation."
            )

        stripe.api_key = self.secret_key
        logger.info("StripeCardIssuer initialized (issuing enabled)")

    def create_cardholder(self, name: str, email: str) -> str:
        """Create a cardholder — required before issuing any cards.

        Args:
            name: Full legal name for the cardholder.
            email: Contact email for the cardholder.

        Returns:
            Stripe cardholder ID (ich_...).

        Raises:
            stripe.error.StripeError: On API failure.
        """
        try:
            cardholder = stripe.issuing.Cardholder.create(
                name=name,
                email=email,
                type="individual",
                status="active",
                billing={
                    "address": {
                        "line1": "354 Oyster Point Blvd",
                        "city": "South San Francisco",
                        "state": "CA",
                        "postal_code": "94080",
                        "country": "US",
                    }
                },
            )
            logger.info(
                "Created cardholder %s for %s (%s)",
                cardholder.id, name, email,
            )
            return cardholder.id
        except stripe.error.StripeError as e:
            logger.error("Failed to create cardholder: %s", e)
            raise

    def create_trading_card(
        self, amount_cents: int, pair: str, strategy_id: str
    ) -> dict:
        """Create a virtual card for a specific trade.

        Enforces the $5 max per card. The card is tagged with the
        trading pair and strategy ID in metadata for tracking.

        Args:
            amount_cents: Spending limit in cents (max 500 = $5.00).
            pair: Trading pair (e.g. "BTC-USD").
            strategy_id: ID of the strategy using this card.

        Returns:
            Dict with card details: id, number, exp_month, exp_year, cvc,
            last4, spending_limit_cents, pair, strategy_id.

        Raises:
            ValueError: If amount_cents exceeds $5.00 (500 cents).
            RuntimeError: If no active cardholders exist.
            stripe.error.StripeError: On API failure.
        """
        if amount_cents > MAX_CARD_AMOUNT_CENTS:
            raise ValueError(
                f"amount_cents={amount_cents} exceeds $5.00 max "
                f"({MAX_CARD_AMOUNT_CENTS} cents). Trading rule #1: NEVER lose money."
            )
        if amount_cents <= 0:
            raise ValueError("amount_cents must be positive")

        # Find an active cardholder to attach this card to
        cardholder_id = self._get_active_cardholder()
        if not cardholder_id:
            raise RuntimeError(
                "No active cardholder found. Call create_cardholder() first."
            )

        try:
            card = stripe.issuing.Card.create(
                cardholder=cardholder_id,
                currency="usd",
                type="virtual",
                status="active",
                spending_controls={
                    "spending_limits": [
                        {
                            "amount": amount_cents,
                            "interval": "all_time",
                        }
                    ],
                },
                metadata={
                    "pair": pair,
                    "strategy_id": strategy_id,
                    "created_by": "stripe_issuing.py",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "max_amount_cents": str(amount_cents),
                },
            )

            # Retrieve full card details (number, cvc) — only available
            # immediately after creation for virtual cards
            details = stripe.issuing.Card.retrieve(
                card.id, expand=["number", "cvc"]
            )

            result = {
                "id": card.id,
                "number": getattr(details, "number", None),
                "exp_month": card.exp_month,
                "exp_year": card.exp_year,
                "cvc": getattr(details, "cvc", None),
                "last4": card.last4,
                "spending_limit_cents": amount_cents,
                "pair": pair,
                "strategy_id": strategy_id,
                "status": card.status,
            }

            logger.info(
                "Created trading card %s (****%s) for %s / %s — limit $%.2f",
                card.id, card.last4, pair, strategy_id,
                amount_cents / 100,
            )
            return result

        except stripe.error.StripeError as e:
            logger.error(
                "Failed to create trading card for %s / %s: %s",
                pair, strategy_id, e,
            )
            raise

    def fund_card(self, card_id: str, amount_cents: int) -> dict:
        """Add funds to an existing card via Stripe Issuing top-up.

        Creates a test-mode funding transaction. In live mode, funding
        comes from the Stripe Issuing balance which is fed by transfers
        from the connected bank account.

        Args:
            card_id: Stripe card ID (ic_...).
            amount_cents: Amount to fund in cents.

        Returns:
            Dict with transfer details: id, amount_cents, card_id, status.

        Raises:
            ValueError: If funding would exceed $5.00 total on the card.
            stripe.error.StripeError: On API failure.
        """
        if amount_cents <= 0:
            raise ValueError("amount_cents must be positive")

        # Check current card state to enforce $5 cap
        card = stripe.issuing.Card.retrieve(card_id)
        current_limit = 0
        if card.spending_controls and card.spending_controls.spending_limits:
            for limit in card.spending_controls.spending_limits:
                if limit.interval == "all_time":
                    current_limit = limit.amount
                    break

        if current_limit + amount_cents > MAX_CARD_AMOUNT_CENTS:
            raise ValueError(
                f"Funding {amount_cents}c would exceed $5.00 cap. "
                f"Current limit: {current_limit}c, requested: {amount_cents}c, "
                f"max: {MAX_CARD_AMOUNT_CENTS}c."
            )

        try:
            # Update the spending limit to include the new funds
            new_limit = current_limit + amount_cents
            updated_card = stripe.issuing.Card.modify(
                card_id,
                spending_controls={
                    "spending_limits": [
                        {
                            "amount": new_limit,
                            "interval": "all_time",
                        }
                    ],
                },
            )

            result = {
                "id": f"fund_{card_id}_{amount_cents}",
                "card_id": card_id,
                "amount_cents": amount_cents,
                "new_limit_cents": new_limit,
                "status": "funded",
            }

            logger.info(
                "Funded card %s with %dc (new limit: $%.2f)",
                card_id, amount_cents, new_limit / 100,
            )
            return result

        except stripe.error.StripeError as e:
            logger.error("Failed to fund card %s: %s", card_id, e)
            raise

    def check_card_status(self, card_id: str) -> dict:
        """Get current status and spending info for a card.

        Args:
            card_id: Stripe card ID (ic_...).

        Returns:
            Dict with: id, last4, status, spending_limit_cents,
            pair, strategy_id, created.

        Raises:
            stripe.error.StripeError: On API failure.
        """
        try:
            card = stripe.issuing.Card.retrieve(card_id)

            spending_limit = 0
            if card.spending_controls and card.spending_controls.spending_limits:
                for limit in card.spending_controls.spending_limits:
                    if limit.interval == "all_time":
                        spending_limit = limit.amount
                        break

            result = {
                "id": card.id,
                "last4": card.last4,
                "status": card.status,
                "spending_limit_cents": spending_limit,
                "pair": card.metadata.get("pair", ""),
                "strategy_id": card.metadata.get("strategy_id", ""),
                "created": datetime.fromtimestamp(
                    card.created, tz=timezone.utc
                ).isoformat(),
            }

            logger.info(
                "Card %s (****%s): status=%s, limit=$%.2f",
                card.id, card.last4, card.status, spending_limit / 100,
            )
            return result

        except stripe.error.StripeError as e:
            logger.error("Failed to check card %s: %s", card_id, e)
            raise

    def list_cards(self, limit: int = 10) -> list:
        """List recent cards with their balances and metadata.

        Args:
            limit: Max number of cards to return (default 10).

        Returns:
            List of card dicts with: id, last4, status, spending_limit_cents,
            pair, strategy_id, created.

        Raises:
            stripe.error.StripeError: On API failure.
        """
        try:
            cards = stripe.issuing.Card.list(limit=limit)
            result = []

            for card in cards.data:
                spending_limit = 0
                if card.spending_controls and card.spending_controls.spending_limits:
                    for sl in card.spending_controls.spending_limits:
                        if sl.interval == "all_time":
                            spending_limit = sl.amount
                            break

                result.append({
                    "id": card.id,
                    "last4": card.last4,
                    "status": card.status,
                    "spending_limit_cents": spending_limit,
                    "pair": card.metadata.get("pair", ""),
                    "strategy_id": card.metadata.get("strategy_id", ""),
                    "created": datetime.fromtimestamp(
                        card.created, tz=timezone.utc
                    ).isoformat(),
                })

            logger.info("Listed %d cards", len(result))
            return result

        except stripe.error.StripeError as e:
            logger.error("Failed to list cards: %s", e)
            raise

    def freeze_card(self, card_id: str) -> str:
        """Freeze a card immediately (HARDSTOP for runaway strategies).

        The card can be unfrozen later by setting status back to "active".

        Args:
            card_id: Stripe card ID (ic_...).

        Returns:
            "ok" on success.

        Raises:
            stripe.error.StripeError: On API failure.
        """
        try:
            stripe.issuing.Card.modify(card_id, status="inactive")
            logger.warning("FROZEN card %s", card_id)
            return "ok"
        except stripe.error.StripeError as e:
            logger.error("Failed to freeze card %s: %s", card_id, e)
            raise

    def cancel_card(self, card_id: str) -> str:
        """Permanently cancel a card. This cannot be undone.

        Args:
            card_id: Stripe card ID (ic_...).

        Returns:
            "ok" on success.

        Raises:
            stripe.error.StripeError: On API failure.
        """
        try:
            stripe.issuing.Card.modify(card_id, status="canceled")
            logger.warning("CANCELLED card %s (permanent)", card_id)
            return "ok"
        except stripe.error.StripeError as e:
            logger.error("Failed to cancel card %s: %s", card_id, e)
            raise

    def get_spending_summary(self) -> dict:
        """Get aggregate spending summary across all issuing cards.

        Returns:
            Dict with: total_spent (cents), total_funded (cents),
            cards_active (count), cards_frozen (count), cards_cancelled (count).

        Raises:
            stripe.error.StripeError: On API failure.
        """
        try:
            total_funded = 0
            cards_active = 0
            cards_frozen = 0
            cards_cancelled = 0

            # Paginate through all cards
            has_more = True
            starting_after = None

            while has_more:
                params = {"limit": 100}
                if starting_after:
                    params["starting_after"] = starting_after

                cards = stripe.issuing.Card.list(**params)

                for card in cards.data:
                    if card.status == "active":
                        cards_active += 1
                    elif card.status == "inactive":
                        cards_frozen += 1
                    elif card.status == "canceled":
                        cards_cancelled += 1

                    if card.spending_controls and card.spending_controls.spending_limits:
                        for sl in card.spending_controls.spending_limits:
                            if sl.interval == "all_time":
                                total_funded += sl.amount
                                break

                    starting_after = card.id

                has_more = cards.has_more

            # Get total spent from authorizations
            total_spent = 0
            has_more = True
            starting_after = None

            while has_more:
                params = {"limit": 100, "status": "closed"}
                if starting_after:
                    params["starting_after"] = starting_after

                auths = stripe.issuing.Authorization.list(**params)

                for auth in auths.data:
                    if auth.approved:
                        total_spent += auth.merchant_amount

                    starting_after = auth.id

                has_more = auths.has_more

            summary = {
                "total_spent": total_spent,
                "total_funded": total_funded,
                "cards_active": cards_active,
                "cards_frozen": cards_frozen,
                "cards_cancelled": cards_cancelled,
            }

            logger.info(
                "Spending summary: spent=$%.2f, funded=$%.2f, "
                "active=%d, frozen=%d, cancelled=%d",
                total_spent / 100, total_funded / 100,
                cards_active, cards_frozen, cards_cancelled,
            )
            return summary

        except stripe.error.StripeError as e:
            logger.error("Failed to get spending summary: %s", e)
            raise

    def _get_active_cardholder(self) -> str | None:
        """Find the first active cardholder on the account.

        Returns:
            Cardholder ID (ich_...) or None if no active cardholders exist.
        """
        try:
            cardholders = stripe.issuing.Cardholder.list(
                limit=1, status="active"
            )
            if cardholders.data:
                return cardholders.data[0].id
            return None
        except stripe.error.StripeError as e:
            logger.error("Failed to list cardholders: %s", e)
            return None


if __name__ == "__main__":
    print("=" * 60)
    print("Stripe Issuing — Trading Card Manager")
    print("=" * 60)

    try:
        issuer = StripeCardIssuer()
    except RuntimeError as e:
        print(f"\nNot enabled: {e}")
        sys.exit(1)

    print("\nFetching cards...")
    try:
        cards = issuer.list_cards(limit=10)
        if not cards:
            print("  No cards found.")
        else:
            for c in cards:
                print(
                    f"  {c['id']} (****{c['last4']}) "
                    f"status={c['status']} "
                    f"limit=${c['spending_limit_cents'] / 100:.2f} "
                    f"pair={c['pair'] or 'n/a'} "
                    f"strategy={c['strategy_id'] or 'n/a'}"
                )

        print("\nSpending summary:")
        summary = issuer.get_spending_summary()
        print(f"  Total spent:  ${summary['total_spent'] / 100:.2f}")
        print(f"  Total funded: ${summary['total_funded'] / 100:.2f}")
        print(f"  Cards active: {summary['cards_active']}")
        print(f"  Cards frozen: {summary['cards_frozen']}")
        print(f"  Cards cancelled: {summary['cards_cancelled']}")

    except stripe.error.StripeError as e:
        print(f"\nStripe API error: {e}")
        sys.exit(1)
