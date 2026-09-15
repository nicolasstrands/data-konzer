"""Merge new public-holiday years into validated country datasets."""
from __future__ import annotations

import argparse
import calendar
import html
import json
import pathlib
import re
import urllib.request
from datetime import date, datetime
from html.parser import HTMLParser

ROOT = pathlib.Path(__file__).parent
DATA_DIR = ROOT / "data"
SOURCES_FILE = ROOT / "links.json"
WEEKDAYS = "Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday"
MONTHS = "|".join(calendar.month_name[1:])
HOLIDAY_PATTERN = re.compile(
    rf"(?P<name>[^:\n]+?):\s*\**\s*(?P<weekday>{WEEKDAYS})\s+"
    rf"(?P<day>\d{{1,2}})\s+(?P<month>{MONTHS})", re.IGNORECASE
)


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def _sharepoint_article(page: str) -> str:
    marker = '"CanvasContent1":'
    start = page.find(marker)
    if start < 0:
        raise ValueError("SharePoint article content was not found")
    value, _ = json.JSONDecoder().raw_decode(page, start + len(marker))
    parser = _TextExtractor()
    parser.feed(html.unescape(value))
    return " ".join(parser.parts)


def parse_govmu_page(page: str, year: int) -> list[dict[str, str]]:
    text = _sharepoint_article(page)
    marker = re.search(r"as follows\s*:", text, re.IGNORECASE)
    if not marker:
        raise ValueError("Could not find the start of the holiday list")
    text = text[marker.end():]
    holidays = []
    for match in HOLIDAY_PATTERN.finditer(text):
        name = " ".join(match["name"].split()).strip(" *")
        holiday_date = datetime.strptime(
            f"{match['day']} {match['month']} {year}", "%d %B %Y"
        ).date()
        stated_weekday = match["weekday"].title()
        if holiday_date.strftime("%A") != stated_weekday:
            raise ValueError(
                f"Weekday mismatch for {name}: source says {stated_weekday}, "
                f"date is {holiday_date:%A}"
            )
        holidays.append({"name": name, "date": holiday_date.isoformat()})
    if not holidays:
        raise ValueError("No holidays were found in the government page")
    return holidays


def fetch_source(source: dict[str, str], year: int) -> list[dict[str, str]]:
    parser = source.get("parser")
    if parser != "govmu":
        raise ValueError(f"Unsupported parser {parser!r} for an unpopulated year")
    request = urllib.request.Request(
        source["url"], headers={"User-Agent": "data-konzer/1.0"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        page = response.read().decode(response.headers.get_content_charset() or "utf-8")
    return parse_govmu_page(page, year)


def validate_holidays(country: str, year: str, holidays: object) -> None:
    if not (year.isdigit() and len(year) == 4):
        raise ValueError(f"{country}: invalid year {year!r}")
    if not isinstance(holidays, list) or not holidays:
        raise ValueError(f"{country} {year}: holiday list is empty or invalid")
    seen = set()
    for holiday in holidays:
        if not isinstance(holiday, dict) or set(holiday) != {"name", "date"}:
            raise ValueError(f"{country} {year}: invalid entry {holiday!r}")
        parsed = date.fromisoformat(holiday["date"])
        if parsed.year != int(year) or not holiday["name"].strip():
            raise ValueError(f"{country} {year}: invalid entry {holiday!r}")
        identity = (holiday["name"], holiday["date"])
        if identity in seen:
            raise ValueError(f"{country} {year}: duplicate holiday {identity!r}")
        seen.add(identity)


def load_json(path: pathlib.Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return value


def run(check_only: bool) -> bool:
    sources = load_json(SOURCES_FILE)["countries"]
    for path in DATA_DIR.glob("public-holidays-*.json"):
        country = path.stem.removeprefix("public-holidays-")
        for year, holidays in load_json(path).items():
            validate_holidays(country, year, holidays)
    any_changed = False
    for country, years in sources.items():
        path = DATA_DIR / f"public-holidays-{country}.json"
        dataset = load_json(path) if path.exists() else {}
        country_changed = False
        for year, source in years.items():
            if year in dataset:
                continue
            if check_only:
                continue
            print(f"Fetching {country} {year} from {source['url']}")
            holidays = fetch_source(source, int(year))
            validate_holidays(country, year, holidays)
            dataset[year] = holidays
            country_changed = any_changed = True
        if country_changed:
            ordered = {year: dataset[year] for year in sorted(dataset)}
            path.write_text(json.dumps(ordered, indent=2) + "\n", encoding="utf-8")
    print("All datasets are valid." if check_only else "Refresh complete.")
    return any_changed


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("--check", action="store_true", help="validate without network access")
    run(cli.parse_args().check)
