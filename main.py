from data_fetcher import BybitDataFetcher
from market_storage import MarketDataStorage
from market_b import SMAIndicator, TrendDetector, MarketAnalyzer
from datetime import datetime

def print_analysis_results(analysis_data: dict):
    """Helper function to print analysis results"""
    summary = analysis_data['market_summary']
    print("\nMarket Analysis Results:")
    print(f"Timestamp: {analysis_data['timestamp']}")
    print(f"Interval: {analysis_data['interval']}")
    print(f"\nTotal assets analyzed: {summary['total_assets']}")
    print(f"Assets in uptrend: {summary['uptrend_percentage']:.1f}% ({summary['assets_in_uptrend']} assets)")
    print(f"Assets in downtrend: {summary['downtrend_percentage']:.1f}% ({summary['assets_in_downtrend']} assets)")
    print(f"Assets in neutral: {summary['neutral_percentage']:.1f}% ({summary['assets_in_neutral']} assets)")
    
    print("\nTop Trending Assets:")
    for asset in analysis_data['top_trending_assets']:
        trend_str = "↑ UPTREND" if asset['trend'] == "uptrend" else "↓ DOWNTREND"
        print(f"{asset['symbol']}: {trend_str}")
        print(f"  Price: {asset['price']:.4f}")
        print(f"  SMA: {asset['sma']:.4f}")
        print(f"  Distance from SMA: {asset['distance']:.1f}%")

def main():
    # Initialize components
    fetcher = BybitDataFetcher()
    storage = MarketDataStorage()
    
    # Create analyzer components
    sma = SMAIndicator(period=100)
    trend_detector = TrendDetector(sma_indicator=sma)
    analyzer = MarketAnalyzer(trend_detector=trend_detector)
    
    # Configuration
    DAYS_OF_HISTORY = 105
    INTERVAL = 'D'
    TOP_N_ASSETS = 20  # Number of top trending assets to store
    
    print(f"Starting market analysis for {DAYS_OF_HISTORY} days of {INTERVAL} candles...")
    
    # Fetch market data
    market_data = fetcher.fetch_all_market_data(
        interval=INTERVAL,
        days=DAYS_OF_HISTORY
    )
    
    # Validate and filter data
    print("\nValidating data...")
    valid_symbols = []
    for symbol, df in market_data.items():
        if df is not None and not df.empty and len(df) >= 100:
            valid_symbols.append(symbol)
        else:
            print(f"Skipping {symbol} due to insufficient data")
    
    market_data = {symbol: market_data[symbol] for symbol in valid_symbols}
    
    # Analyze market
    print(f"\nAnalyzing market trends for {len(market_data)} valid symbols...")
    analysis = analyzer.analyze_market(market_data)
    
    # Save analysis results
    storage.save_analysis(
        market_stats=analysis['market_stats'],
        assets_analysis=analysis['assets_analysis'],
        interval=INTERVAL,
        top_n=TOP_N_ASSETS
    )
    
    # Load and print the latest analysis
    latest_analysis = storage.load_latest_analysis(INTERVAL)
    if latest_analysis:
        print_analysis_results(latest_analysis)
    
    # Compare with previous analysis if available
    previous_analysis = storage.load_latest_analysis(INTERVAL)
    if previous_analysis and previous_analysis['timestamp'] != latest_analysis['timestamp']:
        print("\nComparison with previous analysis:")
        prev_summary = previous_analysis['market_summary']
        curr_summary = latest_analysis['market_summary']
        
        uptrend_change = curr_summary['uptrend_percentage'] - prev_summary['uptrend_percentage']
        print(f"Uptrend change: {uptrend_change:+.1f}%")
        
        downtrend_change = curr_summary['downtrend_percentage'] - prev_summary['downtrend_percentage']
        print(f"Downtrend change: {downtrend_change:+.1f}%")

if __name__ == "__main__":
    main()