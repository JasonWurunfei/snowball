# snowball
Snowball is a tool to systematically fetch stock data from the internet and manage it locally.

## To use
1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Configure your watchlist and storage path in `config.yaml`.

3. Run the roller to fetch and update data:

   ```python
   from roller import Roller

   r = Roller()
   r.roll()
   ```

4. To backfill missing data, use:

   ```python
    r.roll_backfill()
    ```

## Configuration
Edit the `config.yaml` file to set your watchlist of stock symbols and the local storage path for the data.

```yaml
storage_path: ./data
... # other configurations
```

## Data Storage
Data is stored in parquet files under the specified storage path, organized by category and symbol. Each symbol has its own directory containing the 1m OHLCV data and metadata.