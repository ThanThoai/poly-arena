"""
Tests for Single Condition Policy (v2 spec Section 2).

Verifies:
  - Both TP+SL set → rejected (ValidationError)
  - Single TP accepted
  - Single SL accepted
  - Neither TP nor SL accepted
"""

import pytest

pytestmark = pytest.mark.skip(reason="TP/SL feature temporarily disabled")
from pydantic import ValidationError

from schemas import BOCreate


def _base_payload(**overrides) -> dict:
    defaults = {
        "symbol": "BTC",
        "timeframe": "M5",
        "forecast": "GREEN",
        "amount": 10.0,
    }
    defaults.update(overrides)
    return defaults


def test_both_tp_and_sl_rejected():
    """Setting both tp_price and sl_price should raise ValidationError."""
    with pytest.raises(ValidationError, match="Only one condition allowed"):
        BOCreate(**_base_payload(tp_price=0.80, sl_price=0.30))


def test_tp_only_accepted():
    """Single TP should be accepted."""
    bo = BOCreate(**_base_payload(tp_price=0.80))
    assert bo.tp_price == 0.80
    assert bo.sl_price is None


def test_sl_only_accepted():
    """Single SL should be accepted."""
    bo = BOCreate(**_base_payload(sl_price=0.30))
    assert bo.sl_price == 0.30
    assert bo.tp_price is None


def test_no_condition_accepted():
    """No TP or SL should be accepted."""
    bo = BOCreate(**_base_payload())
    assert bo.tp_price is None
    assert bo.sl_price is None


def test_tp_price_boundaries():
    """TP price must be between 0 and 1 exclusive."""
    with pytest.raises(ValidationError, match="price must be between"):
        BOCreate(**_base_payload(tp_price=0.0))
    with pytest.raises(ValidationError, match="price must be between"):
        BOCreate(**_base_payload(tp_price=1.0))


def test_sl_price_boundaries():
    """SL price must be between 0 and 1 exclusive."""
    with pytest.raises(ValidationError, match="price must be between"):
        BOCreate(**_base_payload(sl_price=0.0))
    with pytest.raises(ValidationError, match="price must be between"):
        BOCreate(**_base_payload(sl_price=1.0))
