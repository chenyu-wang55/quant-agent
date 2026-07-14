from __future__ import annotations

import pytest

from services.execution.broker_adapter import AlpacaBrokerAdapter, BrokerAdapterError, BrokerOrderPlacement


class CapturingAlpacaAdapter(AlpacaBrokerAdapter):
    def __init__(self) -> None:
        super().__init__(api_key="key", api_secret="secret")
        self.calls: list[dict] = []

    def _request(self, method, path, *, payload=None, query=None):  # type: ignore[no-untyped-def]
        self.calls.append({"method": method, "path": path, "payload": payload, "query": query})
        if method == "DELETE":
            return {}
        if path == "/v2/account":
            return {
                "id": "acct_123",
                "status": "ACTIVE",
                "currency": "USD",
                "cash": "1234.56",
                "buying_power": "9876.54",
                "equity": "12000.10",
                "portfolio_value": "11950.25",
                "trading_blocked": False,
                "account_blocked": False,
                "transfers_blocked": "false",
                "pattern_day_trader": "true",
            }
        if path == "/v2/positions":
            return [
                {
                    "asset_id": "asset_aapl",
                    "symbol": "AAPL",
                    "qty": "4.5",
                    "avg_entry_price": "181.23",
                    "current_price": "184.56",
                }
            ]
        return {
            "id": "alpaca_order_123",
            "status": "accepted",
            "client_order_id": (
                payload.get("client_order_id")
                if payload
                else (query or {}).get("client_order_id")
            ),
            "submitted_at": "2026-04-10T15:30:00Z",
        }


def test_alpaca_adapter_submits_limit_order_payload() -> None:
    adapter = CapturingAlpacaAdapter()

    update = adapter.submit_order(
        BrokerOrderPlacement(
            client_order_id="quant_test_123",
            symbol="aapl",
            qty=3,
            side="BUY",
            limit_price=178.25,
        )
    )

    assert update.broker_order_id == "alpaca_order_123"
    assert update.raw_status == "accepted"
    assert adapter.calls == [
        {
            "method": "POST",
            "path": "/v2/orders",
            "payload": {
                "symbol": "AAPL",
                "qty": "3",
                "side": "buy",
                "type": "limit",
                "time_in_force": "day",
                "client_order_id": "quant_test_123",
                "limit_price": "178.25",
            },
            "query": None,
        }
    ]


def test_alpaca_adapter_gets_order_by_client_order_id() -> None:
    adapter = CapturingAlpacaAdapter()

    update = adapter.get_order_by_client_order_id("quant_test_456")

    assert update.client_order_id == "quant_test_456"
    assert adapter.calls[0]["method"] == "GET"
    assert adapter.calls[0]["path"] == "/v2/orders:by_client_order_id"
    assert adapter.calls[0]["query"] == {"client_order_id": "quant_test_456"}


def test_alpaca_adapter_submits_native_bracket_protection() -> None:
    adapter = CapturingAlpacaAdapter()
    adapter.submit_order(
        BrokerOrderPlacement(
            client_order_id="quant_bracket_test",
            symbol="AAPL",
            qty=2,
            side="BUY",
            limit_price=180,
            order_class="bracket",
            take_profit_limit_price=210,
            stop_loss_price=168,
        )
    )

    payload = adapter.calls[0]["payload"]
    assert payload["order_class"] == "bracket"
    assert payload["take_profit"] == {"limit_price": "210"}
    assert payload["stop_loss"] == {"stop_price": "168"}


def test_alpaca_adapter_gets_order_by_broker_order_id() -> None:
    adapter = CapturingAlpacaAdapter()

    update = adapter.get_order_by_id("alpaca/order 456")

    assert update.broker_order_id == "alpaca_order_123"
    assert adapter.calls[0]["method"] == "GET"
    assert adapter.calls[0]["path"] == "/v2/orders/alpaca%2Forder%20456"
    assert adapter.calls[0]["query"] is None


def test_alpaca_adapter_cancels_order_by_broker_order_id() -> None:
    adapter = CapturingAlpacaAdapter()

    update = adapter.cancel_order("alpaca/order 789")

    assert update.broker_order_id == "alpaca/order 789"
    assert update.raw_status == "canceled"
    assert "cancel request accepted" in (update.message or "")
    assert adapter.calls[0]["method"] == "DELETE"
    assert adapter.calls[0]["path"] == "/v2/orders/alpaca%2Forder%20789"
    assert adapter.calls[0]["query"] is None


def test_alpaca_adapter_lists_positions() -> None:
    adapter = CapturingAlpacaAdapter()

    positions = adapter.list_positions()

    assert len(positions) == 1
    assert positions[0].symbol == "AAPL"
    assert positions[0].qty == 4.5
    assert positions[0].avg_price == 181.23
    assert positions[0].market_price == 184.56
    assert positions[0].broker_position_id == "asset_aapl"
    assert adapter.calls[0]["method"] == "GET"
    assert adapter.calls[0]["path"] == "/v2/positions"


def test_alpaca_adapter_gets_account_snapshot() -> None:
    adapter = CapturingAlpacaAdapter()

    account = adapter.get_account()

    assert account.account_id == "acct_123"
    assert account.status == "ACTIVE"
    assert account.currency == "USD"
    assert account.cash == 1234.56
    assert account.buying_power == 9876.54
    assert account.equity == 12000.10
    assert account.portfolio_value == 11950.25
    assert account.trading_blocked is False
    assert account.account_blocked is False
    assert account.transfers_blocked is False
    assert account.pattern_day_trader is True
    assert adapter.calls[0]["method"] == "GET"
    assert adapter.calls[0]["path"] == "/v2/account"


def test_alpaca_adapter_rejects_short_positions_for_long_only_ledger() -> None:
    with pytest.raises(BrokerAdapterError, match="short position"):
        AlpacaBrokerAdapter._to_position(
            {
                "asset_id": "asset_tsla",
                "symbol": "TSLA",
                "qty": "2",
                "side": "short",
                "avg_entry_price": "200",
            }
        )


def test_alpaca_adapter_submits_sell_order_payload() -> None:
    adapter = CapturingAlpacaAdapter()

    adapter.submit_order(
        BrokerOrderPlacement(
            client_order_id="quant_sell_test",
            symbol="MSFT",
            qty=2.5,
            side="SELL",
            limit_price=420.12,
        )
    )

    payload = adapter.calls[0]["payload"]
    assert payload["symbol"] == "MSFT"
    assert payload["qty"] == "2.5"
    assert payload["side"] == "sell"
    assert payload["type"] == "limit"
    assert payload["limit_price"] == "420.12"
