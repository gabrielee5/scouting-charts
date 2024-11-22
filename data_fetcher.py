from pybit.unified_trading import HTTP
import pandas as pd
import logging
import time
from typing import Optional
from datetime import datetime, timedelta
from pathlib import Path
import json
from dotenv import dotenv_values

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BybitDataFetcher:
    def __init__(self, cache_dir: str = "cache"):
        # Load API credentials from .env file
        self.secrets = dotenv_values(".env")
        self.api_key = self.secrets["BYBIT_API_KEY"]
        self.api_secret = self.secrets["BYBIT_API_SECRET"]
        
        # Initialize session with API credentials
        self.session = HTTP(
            api_key=self.api_key,
            api_secret=self.api_secret
        )
        
        # Set up cache directory
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        # Cache duration (4 hours)
        self.cache_duration = timedelta(hours=4)
        
        # Maximum points per API request (Bybit limit)
        self.MAX_POINTS_PER_REQUEST = 1000
        
        logging.info("Bybit data fetcher initialized successfully")
    
    def _get_cache_path(self, symbol: str, interval: str) -> Path:
        """Generate cache file path for a symbol and interval"""
        # Convert numeric interval to string representation
        interval_str = interval if isinstance(interval, str) else f"{interval}min"
        return self.cache_dir / f"{symbol}_{interval_str}.parquet"
    
    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache file exists and is recent enough"""
        if not cache_path.exists():
            return False
        
        cache_time = datetime.fromtimestamp(cache_path.stat().st_mtime)
        return datetime.now() - cache_time < self.cache_duration

    def calculate_required_points(self, days: int, interval: str) -> int:
        """
        Calculate how many data points needed for specified number of days
        
        Args:
            days: Number of days of historical data wanted
            interval: Candlestick interval ('1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'W', 'M')
        
        Returns:
            Number of data points needed
        """
        # Handle different interval types
        if interval in ['D', 'W', 'M']:
            if interval == 'D':
                points = days
            elif interval == 'W':
                points = days // 7 + 1
            else:  # Monthly
                points = days // 30 + 1
        else:
            # Convert string numeric interval to int if needed
            interval_minutes = int(interval) if isinstance(interval, str) else interval
            points = (days * 24 * 60) // interval_minutes

        return min(points, 1000)  # Bybit limit is 1000 points per request

    def get_kline_data(self, 
                      symbol: str, 
                      interval: str = '60', 
                      days: int = 30, 
                      use_cache: bool = True) -> pd.DataFrame:
        """
        Fetch kline/candlestick data for a symbol using Bybit's HTTP client
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Candlestick interval ('1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'W', 'M')
            days: Number of days of historical data to fetch
            use_cache: Whether to use cached data if available
        """
        cache_path = self._get_cache_path(symbol, interval)
        
        # Try to load from cache first
        if use_cache and self._is_cache_valid(cache_path):
            try:
                df = pd.read_parquet(cache_path)
                # Verify we have enough data points
                required_points = self.calculate_required_points(days, interval)
                if len(df) >= required_points:
                    return df.tail(required_points)
            except Exception as e:
                logging.warning(f"Error reading cache for {symbol}: {e}")
        
        # Fetch fresh data if cache is invalid or not used
        try:
            end_time = int(time.time() * 1000)
            points_needed = self.calculate_required_points(days, interval)
            
            # Calculate start time based on interval type
            if interval in ['D', 'W', 'M']:
                start_time = end_time - (days * 24 * 60 * 60 * 1000)
            else:
                interval_minutes = int(interval) if isinstance(interval, str) else interval
                start_time = end_time - (interval_minutes * 60 * 1000 * points_needed)
            
            logging.info(f"Fetching {points_needed} points for {symbol}")
            
            response = self.session.get_kline(
                category="linear",
                symbol=symbol,
                interval=interval,
                start=start_time,
                end=end_time,
                limit=points_needed
            )
            
            if not response.get('result', {}).get('list'):
                logging.warning(f"No kline data returned for {symbol}")
                return pd.DataFrame()
                
            df = pd.DataFrame(
                response['result']['list'],
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover']
            )
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_numeric(df['timestamp'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Convert other columns to numeric
            for col in ['open', 'high', 'low', 'close', 'volume', 'turnover']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Sort data by timestamp in ascending order
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # Cache the data
            if use_cache:
                try:
                    df.to_parquet(cache_path)
                except Exception as e:
                    logging.warning(f"Error saving cache for {symbol}: {e}")
            
            return df
                
        except Exception as e:
            logging.error(f"Error fetching kline data for {symbol}: {str(e)}")
            return pd.DataFrame()

    def get_all_symbols(self) -> list[str]:
        """Fetch all available trading symbols from Bybit"""
        cache_path = self.cache_dir / "symbols.json"
        
        # Try to load from cache first
        if self._is_cache_valid(cache_path):
            try:
                with open(cache_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.warning(f"Error reading symbols cache: {e}")
        
        try:
            response = self.session.get_instruments_info(
                category="linear",
                status="Trading"
            )
            
            symbols = [
                item['symbol'] for item in response['result']['list']
                if 'USDT' in item['symbol']
            ]
            
            # Cache the symbols
            try:
                with open(cache_path, 'w') as f:
                    json.dump(symbols, f)
            except Exception as e:
                logging.warning(f"Error saving symbols cache: {e}")
            
            logging.info(f"Successfully fetched {len(symbols)} symbols")
            return symbols
            
        except Exception as e:
            logging.error(f"Error fetching symbols: {str(e)}")
            return []

    def fetch_all_market_data(self, 
                            interval: str = '60', 
                            days: int = 30) -> dict[str, pd.DataFrame]:
        """
        Fetch kline data for all available symbols
        
        Args:
            interval: Candlestick interval ('1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'W', 'M')
            days: Number of days of historical data to fetch
            
        Returns:
            Dictionary mapping symbols to their historical data DataFrames
        """
        market_data = {}
        symbols = self.get_all_symbols()
        
        if not symbols:
            logging.error("No symbols fetched, aborting scan")
            return {}
        
        total_symbols = len(symbols)
        for idx, symbol in enumerate(symbols, 1):
            logging.info(f"Fetching data for {symbol} ({idx}/{total_symbols})")
            
            df = self.get_kline_data(symbol, interval, days)
            if not df.empty:
                market_data[symbol] = df
            
            # Add a small delay to avoid hitting rate limits
            time.sleep(0.1)
        
        return market_data

def main():
    # Create fetcher instance
    fetcher = BybitDataFetcher()
    
    # Configuration
    DAYS_OF_HISTORY = 7
    INTERVAL = 'D'  # Use 'D' for daily candles
    
    print(f"Starting data collection for {DAYS_OF_HISTORY} days of {INTERVAL} candles...")
    start_time = time.time()
    
    # Fetch all market data
    market_data = fetcher.fetch_all_market_data(
        interval=INTERVAL,
        days=DAYS_OF_HISTORY
    )
    
    end_time = time.time()
    print(f"\nData collection completed in {end_time - start_time:.2f} seconds")
    print(f"Collected data for {len(market_data)} symbols")
    
    # Show example of data collected
    for symbol, df in list(market_data.items())[:3]:  # Show first 3 symbols
        print(f"\n{symbol}:")
        print(f"  Shape: {df.shape}")
        print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"  Number of candlesticks: {len(df)}")

if __name__ == "__main__":
    main()