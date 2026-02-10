from __future__ import annotations

from pathlib import Path

from src.momentum_weekly.config_utils import ensure_dir, load_config
from src.momentum_weekly.data_provider import create_provider
from src.momentum_weekly.io_utils import write_table


def chunked(items: list[str], size: int):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def main() -> None:
    cfg = load_config("config.yaml")
    provider = create_provider(cfg)

    data_cfg = cfg["data"]
    raw_dir = ensure_dir(data_cfg["raw_dir"])

    symbols = provider.get_universe(int(data_cfg["num_stocks"]))
    chunk_size = int(data_cfg.get("fetch_chunk_size", 50))

    chunk_files: list[Path] = []
    print(f"[fetch_data] provider={data_cfg['provider']} symbols={len(symbols)}")
    for chunk_idx, symbol_chunk in enumerate(chunked(symbols, chunk_size), start=1):
        chunk_df = provider.get_price_data(
            symbols=symbol_chunk,
            start_date=str(data_cfg["start_date"]),
            end_date=str(data_cfg["end_date"]),
        )

        file_path = raw_dir / f"prices_chunk_{chunk_idx:03d}.parquet"
        write_table(file_path, chunk_df)
        chunk_files.append(file_path)

        print(
            f"[fetch_data] chunk={chunk_idx:03d} rows={len(chunk_df)} file={file_path}"
        )

    universe_path = raw_dir / "universe.parquet"
    write_table(
        universe_path,
        [{"symbol": symbol, "in_universe": 1} for symbol in symbols],
    )
    print(f"[fetch_data] universe file={universe_path}")
    print(f"[fetch_data] done. total_chunks={len(chunk_files)}")


if __name__ == "__main__":
    main()
