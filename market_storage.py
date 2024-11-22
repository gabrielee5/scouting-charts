from pathlib import Path
import json
from datetime import datetime
import logging
from typing import Dict, List

class MarketDataStorage:
    def __init__(self, base_dir: str = "market_data"):
        """Initialize market data storage"""
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        logging.info(f"Initialized market data storage in {self.base_dir}")
    
    def save_analysis(self, 
                     market_stats: Dict,
                     assets_analysis: Dict,
                     interval: str,
                     top_n: int = 10) -> None:
        """
        Save market analysis results
        
        Args:
            market_stats: Dictionary containing market statistics
            assets_analysis: Dictionary containing individual asset analysis
            interval: Time interval of the data
            top_n: Number of top trending assets to store
        """
        timestamp = datetime.now()
        
        # Sort assets by trend and distance from SMA
        trending_assets = []
        for symbol, data in assets_analysis.items():
            if abs(data['current_trend']) == 1:  # Only consider clear trends
                trend_type = "uptrend" if data['current_trend'] == 1 else "downtrend"
                price = data['last_price']
                sma = data['sma']
                distance = abs(price - sma)/sma * 100
                
                trending_assets.append({
                    'symbol': symbol,
                    'trend': trend_type,
                    'price': price,
                    'sma': sma,
                    'distance': distance
                })
        
        # Sort by distance from SMA
        trending_assets.sort(key=lambda x: x['distance'], reverse=True)
        
        # Prepare data for storage
        analysis_data = {
            'timestamp': timestamp.isoformat(),
            'interval': interval,
            'market_summary': {
                'total_assets': market_stats['total_assets'],
                'assets_in_uptrend': int(market_stats['total_assets'] * market_stats['uptrend_percentage'] / 100),
                'assets_in_downtrend': int(market_stats['total_assets'] * market_stats['downtrend_percentage'] / 100),
                'assets_in_neutral': int(market_stats['total_assets'] * market_stats['neutral_percentage'] / 100),
                'uptrend_percentage': market_stats['uptrend_percentage'],
                'downtrend_percentage': market_stats['downtrend_percentage'],
                'neutral_percentage': market_stats['neutral_percentage']
            },
            'top_trending_assets': trending_assets[:top_n]
        }
        
        # Save to file
        filename = self.base_dir / f"market_analysis_{interval}_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(analysis_data, f, indent=2)
        
        logging.info(f"Saved market analysis to {filename}")
    
    def load_latest_analysis(self, interval: str) -> Dict:
        """
        Load the most recent market analysis for given interval
        """
        analysis_files = list(self.base_dir.glob(f"market_analysis_{interval}_*.json"))
        if not analysis_files:
            logging.warning(f"No analysis found for interval {interval}")
            return {}
        
        latest_file = max(analysis_files, key=lambda x: x.stat().st_mtime)
        
        try:
            with open(latest_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading analysis: {e}")
            return {}