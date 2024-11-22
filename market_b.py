from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging

class Indicator(ABC):
    """Abstract base class for all indicators"""
    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate the indicator values"""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the indicator name"""
        pass

class SMAIndicator(Indicator):
    def __init__(self, period: int = 100):
        self.period = period
    
    @property
    def name(self) -> str:
        return f"SMA_{self.period}"
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate SMA on the data"""
        # Make a copy to avoid modifying original
        df = df.copy()
        
        # Ensure column names are capitalized
        df = df.rename(columns={col: col.capitalize() for col in df.columns})
        
        # Calculate SMA
        df['SMA'] = df['Close'].rolling(window=self.period).mean()
        
        return df

class TrendDetector:
    def __init__(self, sma_indicator: Indicator):
        self.sma_indicator = sma_indicator
    
    def detect_trend(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect trend using SMA position and direction
        Uptrend: price above SMA and SMA rising
        Downtrend: price below SMA and SMA falling
        Neutral: all other cases
        """
        # Calculate SMA
        result_df = self.sma_indicator.calculate(df)
        
        # Calculate SMA slope (positive = rising, negative = falling)
        result_df['SMA_Change'] = result_df['SMA'].diff()
        
        # Determine trend based on price position relative to SMA and SMA direction
        trend = pd.Series(0, index=df.index)  # Initialize all as neutral
        
        # Uptrend: price above SMA and SMA rising
        trend[(result_df['Close'] > result_df['SMA']) & 
              (result_df['SMA_Change'] > 0)] = 1
        
        # Downtrend: price below SMA and SMA falling
        trend[(result_df['Close'] < result_df['SMA']) & 
              (result_df['SMA_Change'] < 0)] = -1
        
        result_df['Trend'] = trend
        
        return result_df

class MarketAnalyzer:
    def __init__(self, trend_detector: TrendDetector):
        """
        Initialize market analyzer
        
        Args:
            trend_detector: TrendDetector instance
        """
        self.trend_detector = trend_detector
    
    def analyze_market(self, 
                      market_data: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
        """
        Analyze entire market and return statistics
        """
        assets_analysis = {}
        valid_assets = 0
        uptrend_count = 0
        downtrend_count = 0
        
        for symbol, df in market_data.items():
            if len(df) < self.trend_detector.sma_indicator.period:  # Skip assets with insufficient data
                continue
                
            # Analyze individual asset
            analysis = self.analyze_asset(df)
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
        
        # Calculate key metrics
        current_trend = analysis_df['Trend'].iloc[-1]
        
        # Calculate trend changes
        trend_changes = (analysis_df['Trend'].diff() != 0).sum()
        
        return {
            'current_trend': current_trend,
            'trend_changes': trend_changes,
            'last_price': analysis_df['Close'].iloc[-1],
            'price_change_24h': 
                ((analysis_df['Close'].iloc[-1] / analysis_df['Close'].iloc[-24] - 1) * 100) 
                if len(analysis_df) >= 24 else None,
            'sma': analysis_df['SMA'].iloc[-1]
        }

def main():
    from data_fetcher import BybitDataFetcher
    
    # Initialize components
    fetcher = BybitDataFetcher()
    
    # Create SMA indicator
    sma = SMAIndicator(period=100)
    
    # Create trend detector
    trend_detector = TrendDetector(sma_indicator=sma)
    
    # Create market analyzer
    analyzer = MarketAnalyzer(trend_detector=trend_detector)
    
    # Fetch market data (ensure we get enough data for SMA calculation)
    print("Fetching market data...")
    market_data = fetcher.fetch_all_market_data(interval='D', days=105)
    
    # Add some basic data validation
    print("\nValidating data...")
    valid_symbols = []
    for symbol, df in market_data.items():
        if df is not None and not df.empty and len(df) >= 100:  # Need at least 100 periods for SMA
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
    print(f"Assets in uptrend (price above rising SMA): {market_stats['uptrend_percentage']:.1f}%")
    print(f"Assets in downtrend (price below falling SMA): {market_stats['downtrend_percentage']:.1f}%")
    print(f"Assets neutral: {market_stats['neutral_percentage']:.1f}%")
    
    # Print trending assets
    print("\nTop trending assets:")
    trending_assets = [
        (symbol, data['current_trend'], data['last_price'], data['sma'])
        for symbol, data in analysis['assets_analysis'].items()
        if abs(data['current_trend']) == 1  # Only show assets in clear trend
    ]
    
    # Sort by price distance from SMA
    trending_assets.sort(key=lambda x: abs(x[2] - x[3])/x[3], reverse=True)
    
    for symbol, trend, price, sma in trending_assets[:5]:
        trend_str = "↑ STRONG UPTREND (above rising SMA)" if trend == 1 else "↓ STRONG DOWNTREND (below falling SMA)"
        distance = abs(price - sma)/sma * 100  # Distance from SMA in percentage
        print(f"{symbol}: {trend_str} (Price: {price:.4f}, SMA: {sma:.4f}, Distance: {distance:.1f}%)")

if __name__ == "__main__":
    main()