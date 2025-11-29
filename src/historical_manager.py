"""
Enhanced Historical Data Management for Multi-Day Analysis
"""

import pandas as pd
import os
import datetime
from config.settings import BASE_DIR

class HistoricalManager:
    def __init__(self):
        self.historical_dir = f"{BASE_DIR}/historical"
        
    def save_daily_data(self, futures_df, options_df, date):
        """Save today's data for future comparison"""
        date_str = date.strftime("%Y-%m-%d")
        
        # Save futures data
        futures_file = f"{self.historical_dir}/futures_{date_str}.csv"
        if futures_df is not None and len(futures_df) > 0:
            futures_df.to_csv(futures_file, index=False)
        
        # Save options data  
        options_file = f"{self.historical_dir}/options_{date_str}.csv"
        if options_df is not None and len(options_df) > 0:
            options_df.to_csv(options_file, index=False)
        
        print(f"💾 Saved historical data for {date_str}")
        
    def load_previous_data(self, current_date):
        """Load previous trading day's data"""
        print(f"🔍 Looking for previous trading day data...")
        
        # Try last 5 days to find previous trading day
        for i in range(1, 6):
            prev_date = current_date - datetime.timedelta(days=i)
            date_str = prev_date.strftime("%Y-%m-%d")
            
            futures_file = f"{self.historical_dir}/futures_{date_str}.csv"
            options_file = f"{self.historical_dir}/options_{date_str}.csv"
            
            if os.path.exists(futures_file) and os.path.exists(options_file):
                try:
                    prev_futures = pd.read_csv(futures_file)
                    prev_options = pd.read_csv(options_file)
                    print(f"✅ Loaded previous data from {date_str}")
                    print(f"   - Futures: {len(prev_futures)} contracts")
                    print(f"   - Options: {len(prev_options)} contracts")
                    return prev_futures, prev_options, prev_date
                except Exception as e:
                    print(f"❌ Error loading {date_str}: {e}")
                    continue
        
        print("⚠️ No previous trading data found")
        return None, None, None
    
    def calculate_oi_change(self, current_futures, prev_futures):
        """Calculate actual OI change from previous day"""
        print("📊 Calculating OI changes from previous day...")
        oi_changes = {}
        
        for _, current in current_futures.iterrows():
            symbol = current['underlying']
            current_oi = current['OpnIntrst']
            
            # Find previous OI for same symbol
            prev_data = prev_futures[prev_futures['underlying'] == symbol]
            if len(prev_data) > 0:
                prev_oi = prev_data.iloc[0]['OpnIntrst']
                oi_change = current_oi - prev_oi
                oi_change_pct = (oi_change / prev_oi) * 100 if prev_oi > 0 else 0
                
                oi_changes[symbol] = {
                    'absolute_change': oi_change,
                    'percentage_change': oi_change_pct,
                    'current_oi': current_oi,
                    'previous_oi': prev_oi,
                    'has_historical_data': True
                }
            else:
                # New symbol, no historical data
                oi_changes[symbol] = {
                    'absolute_change': current['ChngInOpnIntrst'],
                    'percentage_change': 0,
                    'current_oi': current_oi,
                    'previous_oi': 0,
                    'has_historical_data': False
                }
                
        print(f"✅ Calculated OI changes for {len(oi_changes)} symbols")
        return oi_changes
    
    def calculate_volume_change(self, current_futures, prev_futures):
        """Calculate volume change from previous day"""
        print("📊 Calculating volume changes from previous day...")
        volume_changes = {}
        
        for _, current in current_futures.iterrows():
            symbol = current['underlying']
            current_volume = current['TtlTradgVol']
            
            # Find previous volume for same symbol
            prev_data = prev_futures[prev_futures['underlying'] == symbol]
            if len(prev_data) > 0:
                prev_volume = prev_data.iloc[0]['TtlTradgVol']
                volume_change = current_volume - prev_volume
                volume_change_pct = ((current_volume - prev_volume) / prev_volume) * 100 if prev_volume > 0 else 0
                
                volume_changes[symbol] = {
                    'current_volume': current_volume,
                    'previous_volume': prev_volume,
                    'absolute_change': volume_change,
                    'change_percentage': volume_change_pct,
                    'has_historical_data': True
                }
            else:
                # New symbol, no historical data
                volume_changes[symbol] = {
                    'current_volume': current_volume,
                    'previous_volume': 0,
                    'absolute_change': 0,
                    'change_percentage': 0,
                    'has_historical_data': False
                }
                
        print(f"✅ Calculated volume changes for {len(volume_changes)} symbols")
        return volume_changes
    
    def calculate_price_change(self, current_futures, prev_futures):
        """Calculate actual price change from previous day's close"""
        print("📊 Calculating price changes from previous day...")
        price_changes = {}
        
        for _, current in current_futures.iterrows():
            symbol = current['underlying']
            current_price = current['LastPric']
            
            # Find previous close for same symbol
            prev_data = prev_futures[prev_futures['underlying'] == symbol]
            if len(prev_data) > 0:
                prev_close = prev_data.iloc[0]['LastPric']  # Use previous day's last price as close
                price_change = current_price - prev_close
                price_change_pct = (price_change / prev_close) * 100 if prev_close > 0 else 0
                
                price_changes[symbol] = {
                    'current_price': current_price,
                    'previous_close': prev_close,
                    'absolute_change': price_change,
                    'change_percentage': price_change_pct,
                    'has_historical_data': True
                }
            else:
                # New symbol, use provided previous close
                prev_close = current['PrvsClsgPric']
                price_change = current_price - prev_close
                price_change_pct = (price_change / prev_close) * 100 if prev_close > 0 else 0
                
                price_changes[symbol] = {
                    'current_price': current_price,
                    'previous_close': prev_close,
                    'absolute_change': price_change,
                    'change_percentage': price_change_pct,
                    'has_historical_data': False
                }
                
        print(f"✅ Calculated price changes for {len(price_changes)} symbols")
        return price_changes
