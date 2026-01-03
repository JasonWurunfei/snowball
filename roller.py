"""Snowball roller module."""

from datetime import date, timedelta
from datetime import datetime
import os
from time import sleep

import pandas as pd
import yaml
import yfinance as yf


class Roller:
    """Snowball roller class."""

    def __init__(self, config_path: str = "config.yml"):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        # load watchlist symbols to download ohlcv data
        self.symbols = list(self.config["watchlist"]["stocks"].keys())
        # create storage path if not exists
        # root path
        self.storage_path = self.config["storage"]["path"]
        os.makedirs(self.storage_path, exist_ok=True)
        # stock data path
        os.makedirs(os.path.join(self.storage_path, "stocks"), exist_ok=True)
        # load storage meta
        self._load_storage_meta()

    def roll(self):
        """Get 1-month OHLCV data for a given date."""

        # check if there are new symbols added to watchlist
        existing_symbols = []
        for symbol in self.symbols:
            if symbol not in self.meta["stocks"]:
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
        data = yf.download(
            tickers=existing_symbols,
            start=d,
            end=d + timedelta(days=1),
            interval="1m",
            group_by="ticker",
        )

        # save new data to storage
        for symbol in existing_symbols:
            if data[symbol].dropna().empty:
                continue
            symbol_data = data[symbol]
            dir_path = os.path.join(self.storage_path, "stocks", f"{symbol}")
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

        self._update_symbol_meta_file(
            symbol,
            {
                "earliest_date": earliest_date.strftime("%Y-%m-%d"),
                "latest_date": latest_date.strftime("%Y-%m-%d"),
                **self.config["watchlist"]["stocks"][symbol],
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
            self.meta.setdefault("stocks", {})
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
        stocks_dir = os.path.join(self.storage_path, "stocks")
        for symbol in os.listdir(stocks_dir):
            symbol_dir = os.path.join(stocks_dir, symbol)
            if os.path.isdir(symbol_dir):
                # load meta file if exists
                meta_file_path = os.path.join(symbol_dir, "meta.yml")
                if os.path.exists(meta_file_path):
                    with open(meta_file_path, "r", encoding="utf-8") as f:
                        symbol_meta = yaml.safe_load(f)
                    self.meta["stocks"][symbol] = symbol_meta

    def _save_storage_meta(self):
        """Save storage metadata."""
        meta_file_path = os.path.join(self.storage_path, "meta.yml")
        with open(meta_file_path, "w", encoding="utf-8") as f:
            yaml.dump(self.meta, f)

    def _create_symbol_dir(self, symbol: str):
        """Create directory for a given symbol."""
        dir_path = os.path.join(self.storage_path, "stocks", f"{symbol}")
        os.makedirs(dir_path, exist_ok=True)
        return dir_path

    def _update_symbol_meta_file(self, symbol: str, entry: dict = None):
        """Update metadata file for a given symbol."""
        dir_path = os.path.join(self.storage_path, "stocks", f"{symbol}")
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
