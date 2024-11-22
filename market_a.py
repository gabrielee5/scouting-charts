from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from typing import Dict, Optional, Callable
import logging

class ResampledIndicator(ABC):
    """Abstract base class for indicators that use resampled data"""
    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate the indicator values"""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the indicator name"""
        pass

class ModifiedATR(ResampledIndicator):
    def __init__(self, period: int = 14, resample_interval: int = 240, multiplier: float = 2.0):
        self.period = period
        self.resample_interval = resample_interval
        self.multiplier = multiplier
    
    @property
    def name(self) -> str:
        return f"ATR_{self.period}_{self.resample_interval}"
    
    def _calculate_true_range(self, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
        """Calculate True Range"""
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    def _calculate_atr(self, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
        """Calculate ATR"""
        tr = self._calculate_true_range(high, low, close)
        return tr.rolling(window=self.period).mean()

    def resample_to_interval(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resample data to higher timeframe"""
        # Ensure we have a copy to avoid modifying original
        df = df.copy()
        
        # Ensure column names are capitalized
        df = df.rename(columns={col: col.capitalize() for col in df.columns})
        
        # Ensure timestamp is datetime
        if 'Timestamp' in df.columns:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            # Set timestamp as index
            df.set_index('Timestamp', inplace=True)
        
        # Perform resampling
        df_resampled = df.resample(f'{self.resample_interval}min').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        })
        
        return df_resampled

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate modified ATR using resampled data"""
        # Make a copy to avoid modifying original
        df = df.copy()
        
        # Ensure column names are capitalized
        df = df.rename(columns={col: col.capitalize() for col in df.columns})
        
        # Store original index if it's not timestamp
        original_index = None
        if 'Timestamp' in df.columns:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            original_index = df.index
            df.set_index('Timestamp', inplace=True)
        
        # Resample data
        resampled = self.resample_to_interval(df.copy())
        
        # Calculate indicators on resampled data
        resampled['ATR'] = self._calculate_atr(
            resampled.High, 
            resampled.Low, 
            resampled.Close
        ) * self.multiplier
        
        resampled['CloseChange'] = resampled.Close.diff()
        resampled['AbsCloseChange'] = resampled['CloseChange'].abs()
        
        # Create result DataFrame
        result_df = df.copy()
        
        # Merge resampled data back to original timeframe
        for col in resampled.columns:
            result_df[f'Resample_{self.resample_interval}_{col}'] = (
                resampled[col].reindex(df.index).ffill()
            )
        
        # Restore original index if needed
        if original_index is not None:
            result_df.reset_index(inplace=True)
            result_df.index = original_index
        
        return result_df

class TrendDetector:
    def __init__(self, 
                 volatility_indicator: ResampledIndicator,
                 trend_period: int = 10,
                 volatility_multiplier: float = 1.0):
        self.volatility_indicator = volatility_indicator
        self.trend_period = trend_period
        self.volatility_multiplier = volatility_multiplier
    
    def detect_trend(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect trend using volatility-based method"""
        # Calculate indicators including resampled ATR
        result_df = self.volatility_indicator.calculate(df)
        
        # Use resampled ATR for trend detection
        atr_col = f'Resample_{self.volatility_indicator.resample_interval}_ATR'
        close_col = f'Resample_{self.volatility_indicator.resample_interval}_Close'
        
        # Calculate moving average on resampled close
        ma = result_df[close_col].rolling(window=self.trend_period).mean()
        
        # Calculate bands using resampled ATR
        upper_band = ma + (result_df[atr_col] * self.volatility_multiplier)
        lower_band = ma - (result_df[atr_col] * self.volatility_multiplier)
        
        # Determine trend
        trend = pd.Series(0, index=df.index)  # Initialize as neutral
        trend[result_df[close_col] > upper_band] = 1  # Uptrend
        trend[result_df[close_col] < lower_band] = -1  # Downtrend
        
        # Add signals to dataframe
        result_df['MA'] = ma
        result_df['UpperBand'] = upper_band
        result_df['LowerBand'] = lower_band
        result_df['Trend'] = trend
        
        return result_df

class MarketAnalyzer:
    def __init__(self, 
                 trend_detector: TrendDetector,
                 min_volatility: Optional[float] = None):
        """
        Initialize market analyzer
        
        Args:
            trend_detector: TrendDetector instance
            min_volatility: Minimum volatility threshold for considering an asset
        """
        self.trend_detector = trend_detector
        self.min_volatility = min_volatility
    
    def analyze_market(self, 
                      market_data: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
        """
        Analyze entire market and return statistics
        
        Returns dict with:
        - Individual asset analysis
        - Market-wide statistics
        """
        assets_analysis = {}
        valid_assets = 0
        uptrend_count = 0
        downtrend_count = 0
        
        for symbol, df in market_data.items():
            if len(df) < 20:  # Skip assets with insufficient data
                continue
                
            # Analyze individual asset
            analysis = self.analyze_asset(df)
            
            # Skip assets with too low volatility if threshold is set
            if (self.min_volatility is not None and 
                analysis['avg_volatility'] < self.min_volatility):
                continue
            
            assets_analysis[symbol] = analysis
            valid_assets += 1
            
            if analysis['current_trend'] == 1:
                uptrend_count += 1
            elif analysis['current_trend'] == -1:
                downtrend_count += 1
        
        # Calculate market statistics
        market_stats = {
            'total_assets': valid_assets,
            'uptrend_percentage': (uptrend_count / valid_assets * 100) if valid_assets > 0 else 0,
            'downtrend_percentage': (downtrend_count / valid_assets * 100) if valid_assets > 0 else 0,
            'neutral_percentage': 
                ((valid_assets - uptrend_count - downtrend_count) / valid_assets * 100) 
                if valid_assets > 0 else 0
        }
        
        return {
            'market_stats': market_stats,
            'assets_analysis': assets_analysis
        }
    
    def analyze_asset(self, df: pd.DataFrame) -> Dict:
        """
        Analyze single asset and return key metrics
        """
        # Get trend analysis
        analysis_df = self.trend_detector.detect_trend(df)
        
        # Get resampled ATR column name
        resample_interval = self.trend_detector.volatility_indicator.resample_interval
        volatility_col = f'Resample_{resample_interval}_ATR'
        
        # Calculate key metrics
        current_trend = analysis_df['Trend'].iloc[-1]  # Changed from 'trend' to 'Trend'
        
        # Handle the case where volatility column might not exist
        avg_volatility = (
            analysis_df[volatility_col].mean() 
            if volatility_col in analysis_df.columns 
            else 0
        )
        current_volatility = (
            analysis_df[volatility_col].iloc[-1] 
            if volatility_col in analysis_df.columns 
            else 0
        )
        
        # Calculate trend changes
        trend_changes = (analysis_df['Trend'].diff() != 0).sum()
        
        # Get close price column
        close_col = 'Close' if 'Close' in analysis_df.columns else 'close'
        
        return {
            'current_trend': current_trend,
            'avg_volatility': avg_volatility,
            'current_volatility': current_volatility,
            'trend_changes': trend_changes,
            'last_price': analysis_df[close_col].iloc[-1],
            'price_change_24h': 
                ((analysis_df[close_col].iloc[-1] / analysis_df[close_col].iloc[-24] - 1) * 100) 
                if len(analysis_df) >= 24 else None
        }

def main():
    from data_fetcher import BybitDataFetcher
    
    # Initialize components
    fetcher = BybitDataFetcher()
    
    # Create modified ATR indicator with resampling
    atr = ModifiedATR(
        period=14,
        resample_interval=240,  # 4-hour resampling
        multiplier=2.0
    )
    
    # Create trend detector
    trend_detector = TrendDetector(
        volatility_indicator=atr,
        trend_period=10,
        volatility_multiplier=1.5
    )
    
    # Create market analyzer
    analyzer = MarketAnalyzer(
        trend_detector=trend_detector,
        min_volatility=0.001
    )
    
    # Fetch market data (ensure we get enough data for resampling)
    print("Fetching market data...")
    market_data = fetcher.fetch_all_market_data(interval=60, days=14)
    
    # Add some basic data validation
    print("\nValidating data...")
    valid_symbols = []
    for symbol, df in market_data.items():
        if df is not None and not df.empty and len(df) >= 24:  # At least 24 periods for 24h change
            valid_symbols.append(symbol)
        else:
            print(f"Skipping {symbol} due to insufficient data")
    
    # Filter to valid symbols only
    market_data = {symbol: market_data[symbol] for symbol in valid_symbols}
    
    # Analyze market
    print(f"\nAnalyzing market trends for {len(market_data)} valid symbols...")
    analysis = analyzer.analyze_market(market_data)
    
    # Print results
    market_stats = analysis['market_stats']
    print("\nMarket Analysis Results:")
    print(f"Total assets analyzed: {market_stats['total_assets']}")
    print(f"Assets in uptrend: {market_stats['uptrend_percentage']:.1f}%")
    print(f"Assets in downtrend: {market_stats['downtrend_percentage']:.1f}%")
    print(f"Assets neutral: {market_stats['neutral_percentage']:.1f}%")
    
    # Print top trending assets
    print("\nTop trending assets:")
    trending_assets = [
        (symbol, data['current_trend'], data['current_volatility'])
        for symbol, data in analysis['assets_analysis'].items()
        if abs(data['current_trend']) == 1  # Only show assets in clear trend
    ]
    
    # Sort by volatility
    trending_assets.sort(key=lambda x: x[2], reverse=True)
    
    for symbol, trend, vol in trending_assets[:5]:
        trend_str = "↑ UPTREND" if trend == 1 else "↓ DOWNTREND"
        print(f"{symbol}: {trend_str} (Volatility: {vol:.4f})")

if __name__ == "__main__":
    main()