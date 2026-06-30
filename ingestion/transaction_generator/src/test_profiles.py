"""
Smoke test for profile generation, converted to pytest.
"""

from collections import Counter
from decimal import Decimal

from ingestion.transaction_generator.src.profiles import (
    MERCHANT_CATEGORIES,
    ProfileFactory,
)


def test_basic_user_creation() -> None:
    """A user should have all expected fields populated."""
    factory = ProfileFactory(seed=42)
    user = factory.make_user()

    assert user.user_id.startswith("user_")
    assert len(user.email_hash) == 64
    assert len(user.phone_hash) == 64
    assert user.country == "IN"
    assert user.kyc_status in ("VERIFIED", "PENDING", "REJECTED")
    assert Decimal("0") <= user.risk_score <= Decimal("1")
    assert user.preferred_payment_method in ("UPI", "CARD", "NETBANKING", "WALLET")


def test_basic_merchant_creation() -> None:
    """A merchant should have all expected fields populated."""
    factory = ProfileFactory(seed=42)
    merchant = factory.make_merchant()

    assert merchant.merchant_id.startswith("merch_")
    assert merchant.category in MERCHANT_CATEGORIES
    assert merchant.country == "IN"
    assert merchant.risk_tier in ("LOW", "MEDIUM", "HIGH")


def test_distributions_look_realistic() -> None:
    """Generate 5000 users and check distributions are roughly as expected."""
    factory = ProfileFactory(seed=42)
    users = factory.make_users(5000)

    cities = Counter(u.city for u in users)
    kyc = Counter(u.kyc_status for u in users)
    methods = Counter(u.preferred_payment_method for u in users)

    assert kyc["VERIFIED"] > kyc["PENDING"] > kyc["REJECTED"]
    assert methods["UPI"] > methods["CARD"]
    assert cities["Bangalore"] > cities["Lucknow"]


def test_seed_determinism_for_non_id_fields() -> None:
    """
    Same seed must produce same NON-ID fields. UUIDs are intentionally
    non-deterministic (uuid4 uses os.urandom which cannot be seeded),
    so we only verify the seedable fields like city, kyc, risk_score.
    """
    f1 = ProfileFactory(seed=123)
    f2 = ProfileFactory(seed=123)

    u1 = f1.make_user()
    u2 = f2.make_user()

    # IDs WILL differ - that is correct behavior for production use.
    assert u1.user_id != u2.user_id, "UUIDs must be unique even with the same seed"

    # But the seedable fields must match.
    assert u1.email_hash == u2.email_hash
    assert u1.phone_hash == u2.phone_hash
    assert u1.city == u2.city
    assert u1.kyc_status == u2.kyc_status
    assert u1.risk_score == u2.risk_score
    assert u1.preferred_payment_method == u2.preferred_payment_method
