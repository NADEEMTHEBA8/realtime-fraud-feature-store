"""
User and merchant profile generation for the fraud detection pipeline.

These profiles seed the transaction generator with realistic Indian fintech
context: KYC statuses, baseline risk scores, geographic distributions, and
merchant categories. Profiles are deterministic when seeded so that test
runs are reproducible.

Design decisions:
- PII (email, phone) is hashed with SHA-256, never stored as plaintext.
  Real fintech would use HMAC with a key, but for a portfolio project,
  plain SHA-256 is sufficient and clearly documents the intent.
- Indian cities are weighted by approximate digital-payment volume so
  Bangalore and Mumbai dominate, matching real PhonePe/Razorpay traffic.
- Faker is seeded per-instance with seed_instance() to keep multiple
  factories isolated. The class-level Faker.seed() shares state globally
  and breaks reproducibility across factory instances.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

from faker import Faker


# ---------------------------------------------------------------------------
# Indian-specific data - cities, merchant categories, payment-method weights
# ---------------------------------------------------------------------------

INDIAN_CITIES: list[tuple[str, int]] = [
    ("Bangalore", 22),
    ("Mumbai", 20),
    ("Delhi", 16),
    ("Hyderabad", 10),
    ("Pune", 8),
    ("Chennai", 7),
    ("Kolkata", 5),
    ("Ahmedabad", 4),
    ("Jaipur", 3),
    ("Lucknow", 2),
    ("Indore", 2),
    ("Chandigarh", 1),
]

MERCHANT_CATEGORIES: list[str] = [
    "GROCERY",
    "ELECTRONICS",
    "RESTAURANT",
    "FUEL",
    "TRAVEL",
    "ENTERTAINMENT",
    "APPAREL",
    "PHARMACY",
    "EDUCATION",
    "UTILITIES",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha256(value: str) -> str:
    """Return the lowercase hex SHA-256 digest of a string."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _weighted_choice(choices: list[tuple[str, int]], rng: random.Random) -> str:
    """Pick one of (value, weight) entries with weighted probability."""
    values, weights = zip(*choices, strict=True)
    return rng.choices(values, weights=weights, k=1)[0]


# ---------------------------------------------------------------------------
# Domain dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class UserProfile:
    """A simulated user. Frozen so a profile cannot be mutated after creation."""

    user_id: str
    email_hash: str
    phone_hash: str
    city: str
    country: str
    account_created_at: datetime
    kyc_status: str
    risk_score: Decimal
    avg_transaction_amount: Decimal
    preferred_payment_method: str


@dataclass(frozen=True)
class MerchantProfile:
    """A simulated merchant."""

    merchant_id: str
    merchant_name: str
    category: str
    city: str
    country: str
    onboarded_at: datetime
    risk_tier: str
    avg_ticket_size: Decimal


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

@dataclass
class ProfileFactory:
    """
    Builds reproducible batches of users and merchants.

    Pass a seed to get deterministic output across factory instances.
    Without a seed, each run produces different profiles.
    """

    seed: int | None = None
    _faker: Faker = field(init=False)
    _rng: random.Random = field(init=False)

    def __post_init__(self) -> None:
        # Use a fresh Faker instance and seed it per-instance, NOT via the
        # class-level Faker.seed() which would share state globally.
        self._faker = Faker("en_IN")
        if self.seed is not None:
            self._faker.seed_instance(self.seed)
            self._rng = random.Random(self.seed)
        else:
            self._rng = random.Random()

    # ----- users -----

    def make_user(self) -> UserProfile:
        email = self._faker.email()
        phone = self._faker.phone_number()
        city = _weighted_choice(INDIAN_CITIES, self._rng)
        kyc = self._rng.choices(
            ["VERIFIED", "PENDING", "REJECTED"],
            weights=[85, 12, 3],
            k=1,
        )[0]
        risk = round(self._rng.betavariate(2, 8), 2)
        avg_amount = Decimal(str(round(self._rng.lognormvariate(7, 0.7), 2)))

        return UserProfile(
            user_id=f"user_{uuid4().hex[:10]}",
            email_hash=_sha256(email),
            phone_hash=_sha256(phone),
            city=city,
            country="IN",
            account_created_at=datetime.now(timezone.utc) - timedelta(days=self._rng.randint(1, 1500)),
            kyc_status=kyc,
            risk_score=Decimal(str(risk)),
            avg_transaction_amount=avg_amount,
            preferred_payment_method=self._rng.choices(
                ["UPI", "CARD", "NETBANKING", "WALLET"],
                weights=[80, 12, 5, 3],
                k=1,
            )[0],
        )

    def make_users(self, n: int) -> list[UserProfile]:
        return [self.make_user() for _ in range(n)]

    # ----- merchants -----

    def make_merchant(self) -> MerchantProfile:
        category = self._rng.choice(MERCHANT_CATEGORIES)
        city = _weighted_choice(INDIAN_CITIES, self._rng)
        risk_tier = self._rng.choices(
            ["LOW", "MEDIUM", "HIGH"],
            weights=[70, 25, 5],
            k=1,
        )[0]

        category_size_floor = {
            "GROCERY": 200, "RESTAURANT": 300, "PHARMACY": 250,
            "FUEL": 800, "APPAREL": 1200, "UTILITIES": 1000,
            "ELECTRONICS": 5000, "TRAVEL": 3500, "ENTERTAINMENT": 500,
            "EDUCATION": 8000,
        }.get(category, 500)
        avg_ticket = Decimal(str(round(category_size_floor * self._rng.uniform(0.7, 2.5), 2)))

        return MerchantProfile(
            merchant_id=f"merch_{uuid4().hex[:10]}",
            merchant_name=self._faker.company(),
            category=category,
            city=city,
            country="IN",
            onboarded_at=datetime.now(timezone.utc) - timedelta(days=self._rng.randint(30, 2000)),
            risk_tier=risk_tier,
            avg_ticket_size=avg_ticket,
        )

    def make_merchants(self, n: int) -> list[MerchantProfile]:
        return [self.make_merchant() for _ in range(n)]
