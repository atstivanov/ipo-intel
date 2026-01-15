import requests
from datetime import date
from src.config import FINNHUB_API_KEY
from src.db import upsert_ipo_events

def fetch_finnhub_ipo_calendar(date_from: date, date_to: date) -> list[dict]:
    # Finnhub IPO calendar endpoint
    # GET /api/v1/calendar/ipo?from=YYYY-MM-DD&to=YYYY-MM-DD&token=...
    url = "https://finnhub.io/api/v1/calendar/ipo"
    params = {
        "from": date_from.isoformat(),
        "to": date_to.isoformat(),
        "token": FINNHUB_API_KEY,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()

    # Finnhub returns {"ipoCalendar":[...]} in most examples
    events = payload.get("ipoCalendar") or payload.get("ipoCalendarData") or payload.get("data") or []
    if not isinstance(events, list):
        return []
    return events

def ingest_finnhub_ipos(conn, date_from: date, date_to: date) -> int:
    events = fetch_finnhub_ipo_calendar(date_from, date_to)
    upsert_ipo_events(conn, events)
    return len(events)
