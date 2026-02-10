from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _can_use_parquet() -> bool:
    try:
        import pandas  # noqa: F401
        import pyarrow  # noqa: F401

        return True
    except ModuleNotFoundError:
        return False


def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        clean: dict[str, Any] = {}
        for key, value in row.items():
            if hasattr(value, "isoformat"):
                clean[key] = value.isoformat()
            else:
                clean[key] = value
        normalized.append(clean)
    return normalized


def write_table(path: str | Path, rows: list[dict[str, Any]]) -> None:
    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)

    if _can_use_parquet():
        import pandas as pd

        frame = pd.DataFrame(_normalize_rows(rows))
        frame.to_parquet(path_obj, index=False)
        return

    payload = {
        "format": "json_fallback",
        "rows": _normalize_rows(rows),
    }
    path_obj.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def read_table(path: str | Path) -> list[dict[str, Any]]:
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path_obj}")

    if _can_use_parquet():
        try:
            import pandas as pd

            frame = pd.read_parquet(path_obj)
            return frame.to_dict(orient="records")
        except Exception:
            pass

    payload = json.loads(path_obj.read_text(encoding="utf-8"))
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        raise ValueError(f"Invalid table rows in {path_obj}")
    return rows
