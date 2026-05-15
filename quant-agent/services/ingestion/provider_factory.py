from __future__ import annotations

import logging
import os

from services.ingestion.interfaces import DataProvider
from services.ingestion.mock_provider import MockMarketDataProvider
from services.ingestion.vendors.yfinance_provider import YFinanceProvider


logger = logging.getLogger(__name__)


class ExternalProviderPlaceholder:
    """Placeholder for production vendor adapters.

    Implementers can replace this with concrete adapters for market data,
    fundamentals, and news while preserving the DataProvider interface.
    """

    def __getattr__(self, item: str):
        raise NotImplementedError(
            f"External provider method '{item}' is not implemented. "
            "Set DATA_PROVIDER=mock or implement a real adapter."
        )


def build_data_provider() -> DataProvider:
    provider = os.getenv("DATA_PROVIDER", "yfinance").lower()
    if provider == "mock":
        return MockMarketDataProvider()
    if provider == "yfinance":
        logger.info("Using yfinance provider for live market data")
        return YFinanceProvider()
    return ExternalProviderPlaceholder()
