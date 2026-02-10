from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from datetime import datetime
from datetime import timedelta


def _business_days(start_date: str, end_date: str) -> list[date]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    days: list[date] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _uniform_from_seed(seed: int) -> float:
    return ((seed % 10_000_019) + 1) / 10_000_020.0


def _box_muller(seed_a: int, seed_b: int) -> float:
    import math

    u1 = max(_uniform_from_seed(seed_a), 1e-9)
    u2 = _uniform_from_seed(seed_b)
    return math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)


def _seed_for(*parts: object) -> int:
    text = "|".join(str(part) for part in parts)
    total = 0
    for char in text:
        total = (total * 131 + ord(char)) % 2_147_483_647
    return total


class BaseDataProvider(ABC):
    @abstractmethod
    def get_universe(self, num_stocks: int) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_price_data(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> list[dict]:
        raise NotImplementedError


class MockDataProvider(BaseDataProvider):
    def __init__(self, seed: int = 42):
        self.seed = seed

    def get_universe(self, num_stocks: int) -> list[str]:
        symbols: list[str] = []
        for idx in range(num_stocks):
            code = idx + 1
            suffix = "SH" if idx % 2 == 0 else "SZ"
            symbols.append(f"STK{code:04d}.{suffix}")
        return symbols

    def get_price_data(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> list[dict]:
        import math

        days = _business_days(start_date, end_date)
        rows: list[dict] = []

        for symbol in symbols:
            base_seed = _seed_for(self.seed, symbol)
            drift = 0.00015 + 0.00025 * _uniform_from_seed(base_seed + 17)
            vol = 0.010 + 0.020 * _uniform_from_seed(base_seed + 31)

            close_price = 40.0 + 20.0 * _uniform_from_seed(base_seed + 59)
            prev_close = close_price

            for idx, day in enumerate(days):
                noise = _box_muller(base_seed + idx * 7 + 101, base_seed + idx * 11 + 203)
                log_ret = drift + vol * noise * 0.6
                close_price = max(0.5, prev_close * math.exp(log_ret))

                gap_noise = _box_muller(base_seed + idx * 13 + 307, base_seed + idx * 17 + 401)
                open_price = max(0.5, prev_close * math.exp(0.25 * vol * gap_noise))

                rows.append(
                    {
                        "date": day.isoformat(),
                        "symbol": symbol,
                        "open": round(open_price, 6),
                        "close": round(close_price, 6),
                        "in_universe": 1,
                    }
                )
                prev_close = close_price

        rows.sort(key=lambda item: (item["symbol"], item["date"]))
        return rows


class TuShareProvider(BaseDataProvider):
    def get_universe(self, num_stocks: int) -> list[str]:
        raise NotImplementedError("TuShare provider will be implemented later.")

    def get_price_data(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> list[dict]:
        raise NotImplementedError("TuShare provider will be implemented later.")


class JoinQuantProvider(BaseDataProvider):
    def get_universe(self, num_stocks: int) -> list[str]:
        raise NotImplementedError("JoinQuant provider will be implemented later.")

    def get_price_data(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> list[dict]:
        raise NotImplementedError("JoinQuant provider will be implemented later.")


def create_provider(config: dict) -> BaseDataProvider:
    provider_name = str(config["data"]["provider"]).lower()
    seed = int(config["project"]["seed"])

    if provider_name == "mock":
        return MockDataProvider(seed=seed)
    if provider_name == "tushare":
        return TuShareProvider()
    if provider_name == "joinquant":
        return JoinQuantProvider()
    raise ValueError(f"Unsupported provider: {provider_name}")
