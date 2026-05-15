from __future__ import annotations

import os


# Keep tests deterministic and offline-friendly.
os.environ.setdefault("DATA_PROVIDER", "mock")
os.environ.setdefault("QUANT_AGENT_ACCESS_PASSWORD", "test-access-password")
