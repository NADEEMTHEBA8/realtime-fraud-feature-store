"""
Pytest tests for the transaction schemas.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

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
        event_timestamp=datetime.now(UTC),
        city="Bangalore",
        country="IN",
    )
    assert tx.transaction_id is not None
    assert tx.amount == Decimal("1499.50")
    assert tx.currency == "INR"


def test_negative_amount_rejected() -> None:
    """Negative amounts must be rejected."""
    with pytest.raises(ValidationError):
        TransactionEvent(
            user_id="user_1",
            merchant_id="merch_1",
            amount=Decimal("-100.00"),
            currency="INR",
            transaction_type=TransactionType.PURCHASE,
            status=TransactionStatus.SUCCESS,
            payment_method=PaymentMethod.UPI,
            event_timestamp=datetime.now(UTC),
        )


def test_invalid_currency_rejected() -> None:
    """Currency must be 3 uppercase letters."""
    with pytest.raises(ValidationError):
        TransactionEvent(
            user_id="user_1",
            merchant_id="merch_1",
            amount=Decimal("100.00"),
            currency="rupees",  # invalid - not 3 uppercase letters
            transaction_type=TransactionType.PURCHASE,
            status=TransactionStatus.SUCCESS,
            payment_method=PaymentMethod.UPI,
            event_timestamp=datetime.now(UTC),
        )


def test_future_timestamp_rejected() -> None:
    """Events from the future must be rejected."""
    with pytest.raises(ValidationError):
        TransactionEvent(
            user_id="user_1",
            merchant_id="merch_1",
            amount=Decimal("100.00"),
            currency="INR",
            transaction_type=TransactionType.PURCHASE,
            status=TransactionStatus.SUCCESS,
            payment_method=PaymentMethod.UPI,
            event_timestamp=datetime.now(UTC) + timedelta(hours=1),
        )


def test_too_many_decimals_rejected() -> None:
    """Amounts with more than 2 decimal places must be rejected."""
    with pytest.raises(ValidationError):
        TransactionEvent(
            user_id="user_1",
            merchant_id="merch_1",
            amount=Decimal("100.123"),  # 3 decimal places
            currency="INR",
            transaction_type=TransactionType.PURCHASE,
            status=TransactionStatus.SUCCESS,
            payment_method=PaymentMethod.UPI,
            event_timestamp=datetime.now(UTC),
        )
