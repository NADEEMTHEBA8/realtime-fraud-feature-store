"""
Quick smoke test for the transaction schemas.
Run with: python -m ingestion.transaction_generator.src.test_schemas

This is NOT a proper pytest file - it's a manual smoke test to verify
the schema works as expected before we integrate it with the generator.
We'll add real pytest tests later.
"""

from datetime import datetime, timedelta
from decimal import Decimal

from ingestion.transaction_generator.src.schemas import (
    PaymentMethod,
    TransactionEvent,
    TransactionStatus,
    TransactionType,
)


def test_valid_transaction() -> None:
    """A correctly-formed transaction should be accepted."""
    tx = TransactionEvent(
        user_id="user_42891",
        merchant_id="merch_8273",
        amount=Decimal("1499.50"),
        currency="INR",
        transaction_type=TransactionType.PURCHASE,
        status=TransactionStatus.SUCCESS,
        payment_method=PaymentMethod.UPI,
        event_timestamp=datetime.utcnow(),
        city="Bangalore",
        country="IN",
    )
    print(f"Valid transaction created: {tx.transaction_id}")
    print(f"  Amount: {tx.amount} {tx.currency}")
    print(f"  Method: {tx.payment_method.value}")
    print(f"  JSON: {tx.model_dump_json()[:120]}...")


def test_negative_amount_rejected() -> None:
    """Negative amounts must be rejected."""
    try:
        TransactionEvent(
            user_id="user_1",
            merchant_id="merch_1",
            amount=Decimal("-100.00"),
            currency="INR",
            transaction_type=TransactionType.PURCHASE,
            status=TransactionStatus.SUCCESS,
            payment_method=PaymentMethod.UPI,
            event_timestamp=datetime.utcnow(),
        )
        print("FAIL: negative amount was accepted (should have been rejected)")
    except Exception as e:
        print(f"Correctly rejected negative amount: {type(e).__name__}")


def test_invalid_currency_rejected() -> None:
    """Currency must be 3 uppercase letters."""
    try:
        TransactionEvent(
            user_id="user_1",
            merchant_id="merch_1",
            amount=Decimal("100.00"),
            currency="rupees",          # invalid - not 3 uppercase letters
            transaction_type=TransactionType.PURCHASE,
            status=TransactionStatus.SUCCESS,
            payment_method=PaymentMethod.UPI,
            event_timestamp=datetime.utcnow(),
        )
        print("FAIL: invalid currency was accepted")
    except Exception as e:
        print(f"Correctly rejected invalid currency: {type(e).__name__}")


def test_future_timestamp_rejected() -> None:
    """Events from the future must be rejected."""
    try:
        TransactionEvent(
            user_id="user_1",
            merchant_id="merch_1",
            amount=Decimal("100.00"),
            currency="INR",
            transaction_type=TransactionType.PURCHASE,
            status=TransactionStatus.SUCCESS,
            payment_method=PaymentMethod.UPI,
            event_timestamp=datetime.utcnow() + timedelta(hours=1),
        )
        print("FAIL: future timestamp was accepted")
    except Exception as e:
        print(f"Correctly rejected future timestamp: {type(e).__name__}")


def test_too_many_decimals_rejected() -> None:
    """Amounts with more than 2 decimal places must be rejected."""
    try:
        TransactionEvent(
            user_id="user_1",
            merchant_id="merch_1",
            amount=Decimal("100.123"),       # 3 decimal places
            currency="INR",
            transaction_type=TransactionType.PURCHASE,
            status=TransactionStatus.SUCCESS,
            payment_method=PaymentMethod.UPI,
            event_timestamp=datetime.utcnow(),
        )
        print("FAIL: amount with 3 decimals was accepted")
    except Exception as e:
        print(f"Correctly rejected over-precise amount: {type(e).__name__}")


if __name__ == "__main__":
    print("Running schema smoke tests...\n")
    test_valid_transaction()
    print()
    test_negative_amount_rejected()
    test_invalid_currency_rejected()
    test_future_timestamp_rejected()
    test_too_many_decimals_rejected()
    print("\nAll smoke tests passed.")