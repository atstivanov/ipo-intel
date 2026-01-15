import os

def env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None or val == "":
        raise ValueError(f"Missing required env var: {name}")
    return val

PG = {
    "host": env("PG_HOST", "localhost"),
    "port": int(env("PG_PORT", "5432")),
    "dbname": env("PG_DB", "ipo"),
    "user": env("PG_USER", "postgres"),
    "password": env("PG_PASSWORD", "postgres"),
}

FINNHUB_API_KEY = env("FINNHUB_API_KEY", "")
