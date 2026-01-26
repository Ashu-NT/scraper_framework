from __future__ import annotations

from typing import Any, Dict, List
import gspread
from google.oauth2.service_account import Credentials

from scraper_framework.core.models import Record, ScrapeJob
from scraper_framework.sinks.base import Sink


_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class GoogleSheetsSink(Sink):
    """
    Writes records to Google Sheets.
    Supports:
      - append mode (default)
      - simple upsert mode based on a key column (default: source_url)
    """

    def write(self, job: ScrapeJob, records: List[Record]) -> None:
        """Write records to Google Sheets."""
        cfg = job.sink_config
        sheet_id = cfg["sheet_id"]
        tab_name = cfg.get("tab", "Sheet1")
        creds_path = cfg["credentials_path"]

        mode = cfg.get("mode", "append")  # append | upsert
        key_field = cfg.get("key_field", "source_url")  # source_url OR a field in record.fields

        ws = self._open_worksheet(sheet_id, tab_name, creds_path)

        # Build header: stable + union of fields
        field_keys = sorted({k for r in records for k in r.fields.keys()})
        header = ["id", "source_url", "scraped_at_utc"] + field_keys

        self._ensure_header(ws, header)

        if not records:
            return

        rows = [self._record_to_row(r, header) for r in records]

        if mode == "upsert":
            self._upsert(ws, header, rows, key_field=key_field)
        else:
            ws.append_rows(rows, value_input_option="USER_ENTERED")

    def _open_worksheet(self, sheet_id: str, tab: str, creds_path: str):
        """Open a Google Sheets worksheet."""
        creds = Credentials.from_service_account_file(creds_path, scopes=_SCOPES)
        client = gspread.authorize(creds)
        sh = client.open_by_key(sheet_id)
        return sh.worksheet(tab)

    def _ensure_header(self, ws, header: List[str]) -> None:
        """Ensure the worksheet has the correct header."""
        existing = ws.row_values(1)
        if existing != header:
            ws.clear()
            ws.append_row(header, value_input_option="USER_ENTERED")

    def _record_to_row(self, r: Record, header: List[str]) -> List[Any]:
        """Convert a record to a row list."""
        base: Dict[str, Any] = {
            "id": r.id,
            "source_url": r.source_url,
            "scraped_at_utc": r.scraped_at_utc,
        }
        base.update(r.fields)
        return [base.get(col, "") for col in header]

    def _upsert(self, ws, header: List[str], rows: List[List[Any]], key_field: str = "source_url") -> None:
        """Perform upsert operation on the worksheet."""
        # Simple upsert:
        # - reads existing key column into a map: key -> row_index
        # - updates existing rows in-place
        # - appends new rows
        key_col_idx = header.index(key_field) + 1  # 1-based for gspread
        existing_keys = ws.col_values(key_col_idx)[1:]  # skip header
        key_to_row = {k: i + 2 for i, k in enumerate(existing_keys) if k}  # row index in sheet

        updates = []
        appends = []

        for row in rows:
            key_val = row[header.index(key_field)]
            if not key_val:
                appends.append(row)
                continue

            existing_row_idx = key_to_row.get(str(key_val))
            if existing_row_idx:
                updates.append((existing_row_idx, row))
            else:
                appends.append(row)

        # Batch update existing rows (row by row range update)
        for row_idx, row_vals in updates:
            rng = f"A{row_idx}:{self._col_to_a1(len(header))}{row_idx}"
            ws.update(rng, [row_vals], value_input_option="USER_ENTERED")

        if appends:
            ws.append_rows(appends, value_input_option="USER_ENTERED")

    def _col_to_a1(self, col_num: int) -> str:
        """Convert column number to A1 notation."""
        # 1 -> A, 26 -> Z, 27 -> AA
        s = ""
        n = col_num
        while n > 0:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s
