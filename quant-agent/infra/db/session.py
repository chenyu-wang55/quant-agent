from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from infra.config import CoreSettings


def get_database_url() -> str:
    return CoreSettings.from_env().database_url


engine = create_engine(get_database_url(), future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)


def dispose_engine() -> None:
    """Close pooled database connections owned by this process."""

    engine.dispose()
