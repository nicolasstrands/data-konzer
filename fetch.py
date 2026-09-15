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
FRENCH_MONTHS = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "août": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "décembre": 12,
}
FRENCH_WEEKDAYS = {
    "lundi": "Monday", "mardi": "Tuesday", "mercredi": "Wednesday",
    "jeudi": "Thursday", "vendredi": "Friday", "samedi": "Saturday",
    "dimanche": "Sunday",
}


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


class _TableExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self.row: list[str] | None = None
        self.cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self.row = []
        elif tag in {"th", "td"} and self.row is not None:
            self.cell = []

    def handle_data(self, data: str) -> None:
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"th", "td"} and self.cell is not None and self.row is not None:
            self.row.append(" ".join("".join(self.cell).split()))
            self.cell = None
        elif tag == "tr" and self.row is not None:
            if self.row:
                self.rows.append(self.row)
            self.row = None


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


def parse_service_public_page(page: str, year: int) -> list[dict[str, str]]:
    """Parse the first (metropolitan France) legal-holidays table for a year."""
    caption = re.search(
        rf"<caption\b[^>]*>.*?Dates des fêtes légales en {year}.*?</caption>",
        page, re.IGNORECASE | re.DOTALL,
    )
    if not caption:
        raise ValueError(f"Could not find the Service-Public table for {year}")
    table_start = page.rfind("<table", 0, caption.start())
    table_end = page.find("</table>", caption.end())
    if table_start < 0 or table_end < 0:
        raise ValueError(f"Could not isolate the Service-Public table for {year}")
    parser = _TableExtractor()
    parser.feed(page[table_start:table_end + len("</table>")])
    date_pattern = re.compile(
        r"^(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)\s+"
        r"(\d{1,2})\s*(?:er\s+)?([a-zéû]+)\s+(\d{4})$", re.IGNORECASE,
    )
    holidays = []
    for row in parser.rows:
        if len(row) != 2:
            continue
        match = date_pattern.match(row[1])
        if not match or int(match[4]) != year:
            continue
        month = FRENCH_MONTHS.get(match[3].lower())
        if month is None:
            raise ValueError(f"Unknown French month in {row[1]!r}")
        holiday_date = date(year, month, int(match[2]))
        if holiday_date.strftime("%A") != FRENCH_WEEKDAYS[match[1].lower()]:
            raise ValueError(f"Weekday mismatch for {row[0]} on {holiday_date}")
        holidays.append({"name": row[0], "date": holiday_date.isoformat()})
    if not holidays:
        raise ValueError(f"No metropolitan France holidays found for {year}")
    return holidays


def fetch_source(source: dict[str, str], year: int) -> list[dict[str, str]]:
    parser = source.get("parser")
    if parser not in {"govmu", "service-public"}:
        raise ValueError(f"Unsupported parser {parser!r} for an unpopulated year")
    request = urllib.request.Request(
        source["url"], headers={"User-Agent": "data-konzer/1.0"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        page = response.read().decode(response.headers.get_content_charset() or "utf-8")
    if parser == "govmu":
        return parse_govmu_page(page, year)
    return parse_service_public_page(page, year)


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
