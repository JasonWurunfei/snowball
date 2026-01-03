"""Snowball roller module."""

from datetime import date, timedelta
from datetime import datetime
import os

import yaml
import yfinance as yf


class Roller:
    """Snowball roller class."""

    def __init__(self, config_path: str = "config.yml"):
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        self.symbols = config["watchlist"]["stocks"]
        self.storage_path = config["storage"]["path"]
        # create storage path if not exists
        # root path
        os.makedirs(self.storage_path, exist_ok=True)
        # stock data path
        os.makedirs(os.path.join(self.storage_path, "stocks"), exist_ok=True)

    def download_1m_ohlcv(self, d: date):
        """Get 1-month OHLCV data for a given date."""
        data = yf.download(
            tickers=self.symbols,
            start=d,
            end=d + timedelta(days=1),
            interval="1m",
            group_by="ticker",
        )

        # save data to storage
        for symbol in self.symbols:
            if data[symbol].dropna().empty:
                continue
            symbol_data = data[symbol]
            # create symbol directory if not exists
            dir_path = self._create_symbol_dir(symbol)
            file_path = os.path.join(
                dir_path, f"{d.strftime('%Y-%m-%d')}_1m_ohlcv.parquet"
            )
            symbol_data.to_parquet(file_path)
            # create meta file if not exists
            self._update_symbol_meta_file(symbol)

        return data

    def _create_symbol_dir(self, symbol: str):
        """Create directory for a given symbol."""
        dir_path = os.path.join(self.storage_path, "stocks", f"{symbol}")
        os.makedirs(dir_path, exist_ok=True)
        return dir_path

    def _update_symbol_meta_file(self, symbol: str):
        """Update metadata file for a given symbol."""
        dir_path = os.path.join(self.storage_path, "stocks", f"{symbol}")
        meta_file_path = os.path.join(dir_path, "meta.yml")
        meta = {"symbol": symbol}
        meta_last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        meta["last_updated"] = meta_last_updated
        if os.path.exists(meta_file_path):
            with open(meta_file_path, "r", encoding="utf-8") as f:
                existing_meta = yaml.safe_load(f)
            existing_meta.update(meta)
            with open(meta_file_path, "w", encoding="utf-8") as f:
                yaml.dump(existing_meta, f)
        else:
            with open(meta_file_path, "w", encoding="utf-8") as f:
                yaml.dump(meta, f)
