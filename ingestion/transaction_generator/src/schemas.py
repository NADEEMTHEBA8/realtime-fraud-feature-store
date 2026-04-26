"""
Transaction event schemas for the fraud detection pipeline.

These schemas define the contract between the transaction generator
and downstream consumers (Kafka, Spark, dbt). Any event that does not
conform to these schemas is rejected at the source.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ---------------------------------------------------------------------------
# Enums - the set of valid values for categorical fields
# ---------------------------------------------------------------------------

class TransactionType(str, Enum):
    """The kind of transaction being performed."""
    PURCHASE = "PURCHASE"
    REFUND = "REFUND"
    TRANSFER = "TRANSFER"
    WITHDRAWAL = "WITHDRAWAL"


class TransactionStatus(str, Enum):
    """The current state of the transaction."""
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class PaymentMethod(str, Enum):
    """How the user paid. UPI dominates in India - ~80% of our traffic."""
    UPI = "UPI"
    CARD = "CARD"
    NETBANKING = "NETBANKING"
    WALLET = "WALLET"


# ---------------------------------------------------------------------------
# Transaction event - the core message that flows through Kafka
# ---------------------------------------------------------------------------

class TransactionEvent(BaseModel):
    """
    A single payment transaction event.

    This is what gets produced by the transaction generator and consumed
    downstream by Spark Structured Streaming. The schema is intentionally
    immutable (frozen=True) - once created, an event cannot be modified.
    Mutability would defeat the purpose of an event-sourced architecture.
    """

    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        json_encoders={
            Decimal: str,         # JSON has no decimal type; serialize as string
            datetime: lambda v: v.isoformat(),
        },
    )

    # Identifiers
    transaction_id: UUID = Field(default_factory=uuid4, description="Globally unique transaction ID")
    user_id: str = Field(..., min_length=1, max_length=64, description="FK to users table")
    merchant_id: str = Field(..., min_length=1, max_length=64, description="FK to merchants table")

    # Money
    amount: Decimal = Field(..., gt=0, description="Transaction amount, must be positive")
    currency: str = Field(..., pattern=r"^[A-Z]{3}$", description="ISO 4217 currency code")

    # Categorical
    transaction_type: TransactionType
    status: TransactionStatus
    payment_method: PaymentMethod

    # Time
    event_timestamp: datetime = Field(..., description="When the transaction occurred (UTC)")
    ingestion_timestamp: datetime = Field(default_factory=datetime.utcnow, description="When the event entered the pipeline")

    # Optional context (PII handled carefully)
    device_id: str | None = Field(default=None, max_length=64, description="Hashed device fingerprint")
    ip_address: str | None = Field(default=None, max_length=45, description="Source IP, masked to /24")
    city: str | None = Field(default=None, max_length=64)
    country: str | None = Field(default=None, pattern=r"^[A-Z]{2}$", description="ISO 3166-1 alpha-2")

    # ----- validators -----

    @field_validator("amount")
    @classmethod
    def validate_amount_precision(cls, v: Decimal) -> Decimal:
        """Reject amounts with more than 2 decimal places (e.g., 100.123)."""
        if v.as_tuple().exponent < -2:
            raise ValueError("amount must have at most 2 decimal places")
        return v

    @field_validator("event_timestamp")
    @classmethod
    def validate_timestamp_not_future(cls, v: datetime) -> datetime:
        """Reject events from the future (clock skew or bug)."""
        now = datetime.utcnow()
        if v.tzinfo is not None:
            now = now.replace(tzinfo=v.tzinfo)
        if v > now:
            raise ValueError("event_timestamp cannot be in the future")
        return v