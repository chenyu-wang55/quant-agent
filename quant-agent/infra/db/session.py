from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_database_url() -> str:
    return os.getenv("DATABASE_URL", "sqlite:///./quant_agent_v2.db")


engine = create_engine(get_database_url(), future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
