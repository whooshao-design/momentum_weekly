from __future__ import annotations

from pathlib import Path

from src.momentum_weekly.config_utils import ensure_dir, load_config
from src.momentum_weekly.io_utils import read_table, write_table


def main() -> None:
    cfg = load_config("config.yaml")
    raw_dir = Path(cfg["data"]["raw_dir"])
    prepared_dir = ensure_dir(cfg["data"]["prepared_dir"])

    chunk_files = sorted(raw_dir.glob("prices_chunk_*.parquet"))
    if not chunk_files:
        raise FileNotFoundError(
            "No raw chunk files found. Please run fetch_data.py first."
        )

    prepared_paths: list[Path] = []
    print(f"[prepare_data] chunks={len(chunk_files)}")
    for chunk_file in chunk_files:
        rows = read_table(chunk_file)
        rows.sort(key=lambda item: (item["symbol"], item["date"]))

        out_file = prepared_dir / chunk_file.name.replace("prices_chunk", "prepared_chunk")
        write_table(out_file, rows)
        prepared_paths.append(out_file)
        print(f"[prepare_data] input={chunk_file.name} rows={len(rows)} -> {out_file.name}")

    universe_src = raw_dir / "universe.parquet"
    if universe_src.exists():
        universe_dst = prepared_dir / "universe.parquet"
        write_table(universe_dst, read_table(universe_src))
        print(f"[prepare_data] copied universe -> {universe_dst}")

    print(f"[prepare_data] done. prepared_chunks={len(prepared_paths)}")


if __name__ == "__main__":
    main()
