"""Snowball roller module."""

from datetime import date, timedelta
from datetime import datetime
import os
from time import sleep

import pandas as pd
import pandas_market_calendars as mcal
import yaml
import yfinance as yf


class Roller:
    """Snowball roller class."""

    def __init__(self, config_path: str = "config.yml"):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        # load watchlist symbols to download ohlcv data
        self.categories = list(self.config["watchlist"].keys())
        self.symbols = []
        for category in self.categories:
            self.symbols.extend(list(self.config["watchlist"][category].keys()))
        # create storage path if not exists
        # root path
        self.storage_path = self.config["storage"]["path"]
        os.makedirs(self.storage_path, exist_ok=True)
        # symbol data paths
        for category in self.categories:
            os.makedirs(os.path.join(self.storage_path, category), exist_ok=True)
        # load storage meta
        self._load_storage_meta()

    def roll(self):
        """Get 1-month OHLCV data for a given date."""

        # check if there are new symbols added to watchlist
        existing_symbols = []
        for symbol in self.symbols:
            category = self._get_symbol_category(symbol)
            if symbol not in self.meta.get(category, {}):
                print(
                    f"New symbol detected: {symbol}, downloading all available 1m OHLCV data..."
                )
                self._download_all_available_1m_ohlcv_for_symbol(symbol)
            else:
                existing_symbols.append(symbol)

        if not existing_symbols:  # no existing symbols to update
            self._update_storage_meta()
            return

        # download latest data for existing symbols
        d = date.today() - timedelta(days=1)  # download data for yesterday

        trading_symbols = []
        for symbol in existing_symbols:
            if self._is_trading_day_for_symbol(symbol, d):
                trading_symbols.append(symbol)
            else:
                print(f"{d} is not a trading day for symbol {symbol}, skipping...")

        print(f"Downloading 1m OHLCV data for {d} for symbols: {trading_symbols}...")
        data = yf.download(
            tickers=trading_symbols,
            start=d,
            end=d + timedelta(days=1),
            interval="1m",
            group_by="ticker",
        )

        # save new data to storage
        for symbol in trading_symbols:
            if data[symbol].dropna().empty:
                continue
            symbol_data = data[symbol]
            category = self._get_symbol_category(symbol)
            dir_path = os.path.join(self.storage_path, category, f"{symbol}")
            file_path = os.path.join(
                dir_path, f"{d.strftime('%Y-%m-%d')}_1m_ohlcv.parquet"
            )
            # save to parquet
            symbol_data.to_parquet(file_path)
            # update symbol meta file
            self._update_symbol_meta_file(
                symbol, {"latest_date": d.strftime("%Y-%m-%d")}
            )

        self._update_storage_meta()
        print("Roll completed.")

    def roll_backfill(self):
        """Backfill all missing 1m OHLCV data for all symbols in watchlist."""
        for symbol in self.symbols:
            print(f"Backfilling 1m OHLCV data for symbol {symbol}...")
            self._download_all_available_1m_ohlcv_for_symbol(symbol)
        self._update_storage_meta()
        print("Backfill completed.")

    def fill_date(self, target_date: date):
        """Fill 1m OHLCV data for a given date for all symbols in watchlist."""
        for symbol in self.symbols:
            category = self._get_symbol_category(symbol)
            meta = self.meta.get(category, {}).get(symbol, {})
            if not meta:
                print(
                    f"Symbol {symbol} not found in storage meta, downloading all available 1m OHLCV data..."
                )
                self._download_all_available_1m_ohlcv_for_symbol(symbol)
                continue

            earliest_date = date.fromisoformat(meta.get("earliest_date"))
            latest_date = date.fromisoformat(meta.get("latest_date"))

            if target_date < earliest_date or target_date > latest_date:
                print(
                    f"Target date {target_date} is out of range for symbol {symbol}, skipping..."
                )
                continue

            dir_path = os.path.join(self.storage_path, category, f"{symbol}")
            file_path = os.path.join(
                dir_path, f"{target_date.strftime('%Y-%m-%d')}_1m_ohlcv.parquet"
            )
            if os.path.exists(file_path):
                print(
                    f"Data for date {target_date} already exists for symbol {symbol}, skipping..."
                )
                continue

            if not self._is_trading_day_for_symbol(symbol, target_date):
                print(
                    f"{target_date} is not a trading day for symbol {symbol}, skipping..."
                )
                continue

            print(f"Downloading 1m OHLCV data for {target_date} for symbol {symbol}...")
            data = yf.download(
                tickers=symbol,
                start=target_date,
                end=target_date + timedelta(days=1),
                interval="1m",
            )

            if data.dropna().empty:
                print(f"No data available for {target_date} for symbol {symbol}.")
                continue

            # save to parquet
            data.to_parquet(file_path)

        self._update_storage_meta()
        print("Fill missing date completed.")

    def _download_all_available_1m_ohlcv_for_symbol(self, symbol: str):
        """Download all available 1m OHLCV data for a given symbol."""
        d = date.today() - timedelta(
            days=28  # 28 for safety margin
        )  # Yahoo Finance only provides last 30 days of 1m data
        data1 = yf.download(
            tickers=symbol, start=d, end=d + timedelta(days=8), interval="1m"
        )

        for _ in range(3):
            sleep(2)  # to avoid rate limit
            d = d + timedelta(days=8)  # Yahoo Finance 1m data max range is 7 days
            data2 = yf.download(
                tickers=symbol, start=d, end=d + timedelta(days=8), interval="1m"
            )
            data1 = pd.concat([data1, data2])

        sleep(1)  # to avoid rate limit

        # create symbol directory if not exists
        dir_path = self._create_symbol_dir(symbol)

        earliest_date = data1.index.date.min()
        latest_date = data1.index.date.max()
        for single_date in pd.Index(data1.index.date).unique():
            daily_data = data1[data1.index.date == single_date]
            file_path = os.path.join(
                dir_path, f"{single_date.strftime('%Y-%m-%d')}_1m_ohlcv.parquet"
            )
            daily_data.to_parquet(file_path)

        # update symbol meta file
        category = self._get_symbol_category(symbol)
        meta = self.meta.get(category, {}).get(symbol, {})
        if meta:
            earliest_date = min(
                earliest_date, date.fromisoformat(meta.get("earliest_date"))
            )

        self._update_symbol_meta_file(
            symbol,
            {
                "earliest_date": earliest_date.strftime("%Y-%m-%d"),
                "latest_date": latest_date.strftime("%Y-%m-%d"),
                **self.config["watchlist"][category][symbol],
            },
        )

    def _load_storage_meta(self):
        """Load storage metadata."""
        meta_file_path = os.path.join(self.storage_path, "meta.yml")
        if os.path.exists(meta_file_path):
            # load existing meta
            with open(meta_file_path, "r", encoding="utf-8") as f:
                self.meta = yaml.safe_load(f)
        else:
            # initialize meta
            self.meta = {}
            self.meta["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.meta["last_updated"] = self.meta["created_at"]
            for category in self.categories:
                self.meta.setdefault(category, {})
            self._scan_storage_meta()
            # save initial meta
            self._save_storage_meta()

    def _update_storage_meta(self, entry: dict = None):
        """Update storage metadata."""
        self.meta["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if entry:
            self.meta.update(entry)
        self._scan_storage_meta()
        self._save_storage_meta()

    def _scan_storage_meta(self):
        """Scan storage metadata."""
        for category in self.categories:
            category_dir = os.path.join(self.storage_path, category)
            for symbol in os.listdir(category_dir):
                symbol_dir = os.path.join(category_dir, symbol)
                if os.path.isdir(symbol_dir):
                    # load meta file if exists
                    meta_file_path = os.path.join(symbol_dir, "meta.yml")
                    if os.path.exists(meta_file_path):
                        with open(meta_file_path, "r", encoding="utf-8") as f:
                            symbol_meta = yaml.safe_load(f)
                        if category not in self.meta:
                            self.meta[category] = {}
                        self.meta[category][symbol] = symbol_meta

    def _save_storage_meta(self):
        """Save storage metadata."""
        meta_file_path = os.path.join(self.storage_path, "meta.yml")
        with open(meta_file_path, "w", encoding="utf-8") as f:
            yaml.dump(self.meta, f)

    def _get_symbol_category(self, symbol: str):
        """Get category for a given symbol."""
        for category in self.categories:
            if symbol in self.config["watchlist"][category]:
                return category
        return None

    def _create_symbol_dir(self, symbol: str):
        """Create directory for a given symbol."""
        category = self._get_symbol_category(symbol)
        if not category:
            raise ValueError(f"Symbol {symbol} not found in any category.")
        dir_path = os.path.join(self.storage_path, category, f"{symbol}")
        os.makedirs(dir_path, exist_ok=True)
        return dir_path

    def _update_symbol_meta_file(self, symbol: str, entry: dict = None):
        """Update metadata file for a given symbol."""
        category = self._get_symbol_category(symbol)
        if not category:
            raise ValueError(f"Symbol {symbol} not found in any category.")
        dir_path = os.path.join(self.storage_path, category, f"{symbol}")
        meta_file_path = os.path.join(dir_path, "meta.yml")

        meta = {"symbol": symbol}
        meta_last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        meta["last_updated"] = meta_last_updated
        if entry:
            meta.update(entry)

        if os.path.exists(meta_file_path):
            with open(meta_file_path, "r", encoding="utf-8") as f:
                existing_meta = yaml.safe_load(f)
            existing_meta.update(meta)
            with open(meta_file_path, "w", encoding="utf-8") as f:
                yaml.dump(existing_meta, f)
        else:
            with open(meta_file_path, "w", encoding="utf-8") as f:
                yaml.dump(meta, f)

    def _is_trading_day_for_symbol(self, symbol: str, d):
        """Check if a given date is a trading day for a given symbol."""
        category = self._get_symbol_category(symbol)
        if not category:
            raise ValueError(f"Symbol {symbol} not found in any category.")
        exchange_name = self.config["watchlist"][category][symbol].get("exchange")
        if not exchange_name:
            raise ValueError(f"Exchange not specified for symbol {symbol}.")
        exchange = mcal.get_calendar(exchange_name)
        return not exchange.valid_days(start_date=d, end_date=d).empty
