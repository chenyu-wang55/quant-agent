from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


def _utc_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _clean_decimal(value: float) -> str:
    return f"{value:.8f}".rstrip("0").rstrip(".")


@dataclass(frozen=True)
class BrokerOrderPlacement:
    client_order_id: str
    symbol: str
    qty: float
    side: str
    limit_price: float | None = None
    time_in_force: str = "day"


@dataclass(frozen=True)
class BrokerOrderUpdate:
    broker_order_id: str
    raw_status: str
    client_order_id: str | None = None
    submitted_at: datetime | None = None
    filled_at: datetime | None = None
    filled_avg_price: float | None = None
    filled_qty: float | None = None
    message: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerPositionUpdate:
    symbol: str
    qty: float
    avg_price: float | None = None
    market_price: float | None = None
    broker_position_id: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


class BrokerAdapterError(RuntimeError):
    pass


class BrokerExecutionAdapter(Protocol):
    name: str

    def submit_order(self, placement: BrokerOrderPlacement) -> BrokerOrderUpdate:
        ...

    def get_order_by_id(self, broker_order_id: str) -> BrokerOrderUpdate:
        ...

    def get_order_by_client_order_id(self, client_order_id: str) -> BrokerOrderUpdate:
        ...

    def cancel_order(self, broker_order_id: str) -> BrokerOrderUpdate:
        ...

    def list_positions(self) -> list[BrokerPositionUpdate]:
        ...


class AlpacaBrokerAdapter:
    name = "alpaca"

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        base_url: str = "https://paper-api.alpaca.markets",
        timeout_seconds: float = 10.0,
    ) -> None:
        if not api_key or not api_secret:
            raise BrokerAdapterError("Alpaca API key and secret are required")
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_env(cls) -> "AlpacaBrokerAdapter":
        return cls(
            api_key=os.environ.get("ALPACA_API_KEY") or os.environ.get("APCA_API_KEY_ID") or "",
            api_secret=os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("APCA_API_SECRET_KEY") or "",
            base_url=os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets"),
            timeout_seconds=float(os.environ.get("ALPACA_TIMEOUT_SECONDS", "10")),
        )

    def submit_order(self, placement: BrokerOrderPlacement) -> BrokerOrderUpdate:
        order_type = "limit" if placement.limit_price is not None else "market"
        payload: dict[str, Any] = {
            "symbol": placement.symbol.upper(),
            "qty": _clean_decimal(placement.qty),
            "side": placement.side.lower(),
            "type": order_type,
            "time_in_force": placement.time_in_force,
            "client_order_id": placement.client_order_id,
        }
        if placement.limit_price is not None:
            payload["limit_price"] = _clean_decimal(placement.limit_price)
        response = self._request("POST", "/v2/orders", payload=payload)
        return self._to_update(response)

    def get_order_by_client_order_id(self, client_order_id: str) -> BrokerOrderUpdate:
        response = self._request(
            "GET",
            "/v2/orders:by_client_order_id",
            query={"client_order_id": client_order_id},
        )
        return self._to_update(response)

    def get_order_by_id(self, broker_order_id: str) -> BrokerOrderUpdate:
        response = self._request("GET", f"/v2/orders/{quote(broker_order_id, safe='')}")
        return self._to_update(response)

    def cancel_order(self, broker_order_id: str) -> BrokerOrderUpdate:
        response = self._request("DELETE", f"/v2/orders/{quote(broker_order_id, safe='')}")
        if isinstance(response, dict) and response:
            return self._to_update(response)
        return BrokerOrderUpdate(
            broker_order_id=broker_order_id,
            raw_status="canceled",
            message="cancel request accepted",
            raw_payload=response if isinstance(response, dict) else {},
        )

    def list_positions(self) -> list[BrokerPositionUpdate]:
        response = self._request("GET", "/v2/positions")
        if not isinstance(response, list):
            raise BrokerAdapterError("Alpaca positions response was not a list")
        positions: list[BrokerPositionUpdate] = []
        for item in response:
            if not isinstance(item, dict):
                raise BrokerAdapterError("Alpaca positions response included a non-object item")
            positions.append(self._to_position(item))
        return positions

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        query: dict[str, str] | None = None,
    ) -> Any:
        query_string = f"?{urlencode(query)}" if query else ""
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = Request(
            f"{self.base_url}{path}{query_string}",
            data=body,
            method=method,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "APCA-API-KEY-ID": self.api_key,
                "APCA-API-SECRET-KEY": self.api_secret,
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                raw_body = response.read().decode("utf-8")
                if not raw_body.strip():
                    return {}
                return json.loads(raw_body)
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise BrokerAdapterError(f"Alpaca API error {exc.code}: {detail}") from exc
        except URLError as exc:
            raise BrokerAdapterError(f"Alpaca API request failed: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise BrokerAdapterError("Alpaca API returned invalid JSON") from exc

    @staticmethod
    def _to_update(payload: dict[str, Any]) -> BrokerOrderUpdate:
        broker_order_id = str(payload.get("id") or payload.get("order_id") or "")
        if not broker_order_id:
            raise BrokerAdapterError("Alpaca response did not include an order id")
        filled_avg_price = payload.get("filled_avg_price")
        filled_qty = payload.get("filled_qty")
        return BrokerOrderUpdate(
            broker_order_id=broker_order_id,
            raw_status=str(payload.get("status") or "unknown").lower(),
            client_order_id=payload.get("client_order_id"),
            submitted_at=_utc_datetime(payload.get("submitted_at") or payload.get("created_at")),
            filled_at=_utc_datetime(payload.get("filled_at")),
            filled_avg_price=float(filled_avg_price) if filled_avg_price not in (None, "") else None,
            filled_qty=float(filled_qty) if filled_qty not in (None, "") else None,
            message=payload.get("message") or payload.get("reject_reason"),
            raw_payload=payload,
        )

    @staticmethod
    def _to_position(payload: dict[str, Any]) -> BrokerPositionUpdate:
        symbol = str(payload.get("symbol") or "").upper().strip()
        if not symbol:
            raise BrokerAdapterError("Alpaca position response did not include a symbol")
        raw_qty = payload.get("qty")
        if raw_qty in (None, ""):
            raise BrokerAdapterError(f"Alpaca position for {symbol} did not include qty")
        qty = float(raw_qty)
        raw_side = str(payload.get("side") or "").lower().strip()
        if qty < 0 or raw_side == "short":
            raise BrokerAdapterError(f"Alpaca short position for {symbol} is not supported by the long-only ledger")
        avg_price = payload.get("avg_entry_price")
        market_price = payload.get("current_price")
        return BrokerPositionUpdate(
            symbol=symbol,
            qty=qty,
            avg_price=float(avg_price) if avg_price not in (None, "") else None,
            market_price=float(market_price) if market_price not in (None, "") else None,
            broker_position_id=payload.get("asset_id"),
            raw_payload=payload,
        )


def build_broker_adapter_from_env() -> BrokerExecutionAdapter | None:
    adapter_name = os.environ.get("QUANT_BROKER_ADAPTER", "").strip().lower()
    if not adapter_name:
        return None
    if adapter_name == "alpaca":
        return AlpacaBrokerAdapter.from_env()
    raise BrokerAdapterError(f"Unsupported broker adapter: {adapter_name}")
