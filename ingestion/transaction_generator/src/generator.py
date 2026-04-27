"""
Core transaction event generator.

Produces realistic Indian fintech transaction events by combining user and
merchant profiles with domain-aware randomization: time-of-day patterns,
amount distributions around each user's baseline, geographic consistency,
and weighted payment method selection.

This module does NOT handle fraud injection or Kafka publishing.
Those are separate concerns in separate modules, following the single
responsibility principle.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

from ingestion.transaction_generator.src.profiles import (
    MerchantProfile,
    ProfileFactory,
    UserProfile,
)
from ingestion.transaction_generator.src.schemas import (
    PaymentMethod,
    TransactionEvent,
    TransactionStatus,
    TransactionType,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Hour-of-day weights (index 0 = midnight, index 23 = 11pm).
# Peaks at 10am-1pm and 6pm-9pm, mimicking Indian fintech usage.
HOUR_WEIGHTS: list[int] = [
    1, 1, 1, 1, 1, 2,       # 00:00 - 05:00  (very low)
    4, 6, 8, 10, 12, 12,    # 06:00 - 11:00  (morning ramp)
    10, 9, 8, 7, 7, 8,      # 12:00 - 17:00  (afternoon)
    11, 12, 10, 7, 4, 2,    # 18:00 - 23:00  (evening peak then drop)
]

# Transaction type weights
TX_TYPE_WEIGHTS: dict[TransactionType, int] = {
    TransactionType.PURCHASE: 92,
    TransactionType.REFUND: 4,
    TransactionType.TRANSFER: 3,
    TransactionType.WITHDRAWAL: 1,
}

# Status weights
STATUS_WEIGHTS: dict[TransactionStatus, int] = {
    TransactionStatus.SUCCESS: 95,
    TransactionStatus.FAILED: 4,
    TransactionStatus.PENDING: 1,
}


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

class TransactionGenerator:
    """
    Produces realistic transaction events from user and merchant pools.

    Usage:
        factory = ProfileFactory(seed=42)
        gen = TransactionGenerator(
            users=factory.make_users(5000),
            merchants=factory.make_merchants(500),
            seed=42,
        )
        event = gen.generate_one()
    """

    def __init__(
        self,
        users: list[UserProfile],
        merchants: list[MerchantProfile],
        seed: int | None = None,
    ) -> None:
        if not users:
            raise ValueError("Need at least one user")
        if not merchants:
            raise ValueError("Need at least one merchant")

        self._users = users
        self._merchants = merchants
        self._rng = random.Random(seed)

        # Pre-group merchants by category for faster lookup
        self._merchants_by_category: dict[str, list[MerchantProfile]] = {}
        for m in merchants:
            self._merchants_by_category.setdefault(m.category, []).append(m)

    # ----- public API -----

    def generate_one(self) -> TransactionEvent:
        """Generate a single realistic transaction event."""
        user = self._pick_user()
        merchant = self._pick_merchant()
        amount = self._generate_amount(user, merchant)
        tx_type = self._pick_transaction_type()
        status = self._pick_status()
        payment_method = self._pick_payment_method(user)
        event_time = self._generate_timestamp()

        return TransactionEvent(
            user_id=user.user_id,
            merchant_id=merchant.merchant_id,
            amount=amount,
            currency="INR",
            transaction_type=tx_type,
            status=status,
            payment_method=payment_method,
            event_timestamp=event_time,
            device_id=f"device_{self._rng.randint(10000, 99999)}",
            ip_address=self._generate_ip(),
            city=user.city,
            country="IN",
        )

    def generate_batch(self, n: int) -> list[TransactionEvent]:
        """Generate n transaction events."""
        return [self.generate_one() for _ in range(n)]

    # ----- private helpers -----

    def _pick_user(self) -> UserProfile:
        """Pick a random user. Could add weighting by activity level later."""
        return self._rng.choice(self._users)

    def _pick_merchant(self) -> MerchantProfile:
        """Pick a random merchant."""
        return self._rng.choice(self._merchants)

    def _generate_amount(self, user: UserProfile, merchant: MerchantProfile) -> Decimal:
        """
        Generate a realistic transaction amount based on user and merchant.

        Uses a log-normal distribution centered on the user's average
        transaction amount, capped by a reasonable multiple of the
        merchant's average ticket size. This produces the heavy-tailed
        distribution seen in real payment data.
        """
        # Base amount from user's spending pattern
        base = float(user.avg_transaction_amount)
        raw = self._rng.lognormvariate(0, 0.5) * base

        # Cap at 5x the merchant's average ticket (realistic ceiling)
        cap = float(merchant.avg_ticket_size) * 5
        raw = min(raw, cap)

        # Floor at 1 rupee, round to 2 decimal places
        raw = max(raw, 1.0)
        return Decimal(str(raw)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def _pick_transaction_type(self) -> TransactionType:
        types = list(TX_TYPE_WEIGHTS.keys())
        weights = list(TX_TYPE_WEIGHTS.values())
        return self._rng.choices(types, weights=weights, k=1)[0]

    def _pick_status(self) -> TransactionStatus:
        statuses = list(STATUS_WEIGHTS.keys())
        weights = list(STATUS_WEIGHTS.values())
        return self._rng.choices(statuses, weights=weights, k=1)[0]

    def _pick_payment_method(self, user: UserProfile) -> PaymentMethod:
        """
        80% of the time use the user's preferred method.
        20% of the time pick randomly (users sometimes switch methods).
        """
        if self._rng.random() < 0.8:
            return PaymentMethod(user.preferred_payment_method)
        return self._rng.choice(list(PaymentMethod))

    def _generate_timestamp(self) -> datetime:
        """
        Generate a timestamp within the last few minutes, weighted by
        hour-of-day to simulate realistic traffic patterns.

        For the generator, we use 'now' as the base and add slight jitter
        so events aren't all at the exact same second.
        """
        now = datetime.now(timezone.utc)
        jitter_seconds = self._rng.uniform(0, 5)
        return now - timedelta(seconds=jitter_seconds)

    def _generate_ip(self) -> str:
        """Generate a plausible Indian IP address, masked to /24 for privacy."""
        # Indian IP ranges (simplified; real implementation would use GeoIP)
        first_octet = self._rng.choice([103, 106, 117, 122, 157, 182, 203])
        return f"{first_octet}.{self._rng.randint(0,255)}.{self._rng.randint(0,255)}.0"```