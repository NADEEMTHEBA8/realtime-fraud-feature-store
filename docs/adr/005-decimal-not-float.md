# ADR-005: NUMERIC for Money, Never FLOAT

## Status
Accepted

## Context
Financial amounts can be represented as FLOAT (binary floating-point) or NUMERIC/DECIMAL (exact decimal). FLOAT has precision errors: `0.1 + 0.2 = 0.30000000000000004`.

## Decision
Use NUMERIC(12,2) for all monetary columns. Use Python's Decimal class in application code. Never use FLOAT or double for money at any layer.

## Consequences
**Positive:**
- Exact precision — ₹1,234.56 is stored and computed exactly
- No rounding errors that compound across millions of transactions
- Passes fintech audit requirements for monetary accuracy

**Negative:**
- NUMERIC is slower than FLOAT for arithmetic operations
- Requires custom JSON serialization (Decimal is not JSON-serializable by default)
- Python's Decimal is more verbose than float

**Trade-off accepted:** Correctness over performance. In fintech, a ₹0.01 rounding error across 10 million daily transactions creates ₹100,000 in reconciliation discrepancies. The performance overhead of NUMERIC is negligible compared to the cost of incorrect financial calculations.
