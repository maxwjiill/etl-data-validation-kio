import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class Settings:
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: str = "5432"


def load_settings() -> Settings:
    env_path = Path(__file__).resolve().parents[3] / ".env"
    load_dotenv(dotenv_path=env_path)

    return Settings(
        postgres_db=os.getenv("POSTGRES_DB", ""),
        postgres_user=os.getenv("POSTGRES_USER", ""),
        postgres_password=os.getenv("POSTGRES_PASSWORD", ""),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=os.getenv("POSTGRES_PORT", "5432"),
    )
