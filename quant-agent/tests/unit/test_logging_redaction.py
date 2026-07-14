from __future__ import annotations

import logging

from infra.observability.logging import SensitiveQueryFilter


def test_password_query_parameter_is_redacted_from_access_log_arguments() -> None:
    record = logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg='%s - "%s %s HTTP/%s" %d',
        args=(
            "127.0.0.1:1234",
            "GET",
            "/dashboard?pwd=super-secret&refresh=true",
            "1.1",
            400,
        ),
        exc_info=None,
    )

    assert SensitiveQueryFilter().filter(record) is True
    rendered = record.getMessage()
    assert "super-secret" not in rendered
    assert "pwd=[REDACTED]" in rendered
