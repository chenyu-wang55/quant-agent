from __future__ import annotations

from services.execution.broker_adapter import AlpacaBrokerAdapter, BrokerOrderPlacement


class CapturingAlpacaAdapter(AlpacaBrokerAdapter):
    def __init__(self) -> None:
        super().__init__(api_key="key", api_secret="secret")
        self.calls: list[dict] = []

    def _request(self, method, path, *, payload=None, query=None):  # type: ignore[no-untyped-def]
        self.calls.append({"method": method, "path": path, "payload": payload, "query": query})
        return {
            "id": "alpaca_order_123",
            "status": "accepted",
            "client_order_id": payload.get("client_order_id") if payload else query.get("client_order_id"),
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
