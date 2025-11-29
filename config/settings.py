"""
Configuration settings for NSE Enhanced Futures + Options Analysis
"""

import os
from datetime import datetime

# Base directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUT_DIR = os.path.join(BASE_DIR, 'outputs')

# Subdirectories
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
SIGNALS_DIR = os.path.join(OUT_DIR, 'signals')
REPORTS_DIR = os.path.join(OUT_DIR, 'reports')

# NSE URLs
NSE_BHAVCOPY_BASE_URL = "https://archives.nseindia.com/content/fo"
NSE_BHAVCOPY_FILENAME_FORMAT = "BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip"

# Data Processing
NSE_COLUMN_MAPPING = {
    'futures': {
        'TckrSymb': 'symbol',
        'UndrlygPric': 'underlying', 
        'LastPric': 'lastPrice',
        'OpnIntrst': 'openInterest',
        'TLITradgVol': 'volume',
        'HghPric': 'highPrice',
        'LwPric': 'lowPrice',
        'OpnPric': 'openPrice',
        'ClsPric': 'closePrice',
        'SttlmPric': 'settlementPrice'
    },
    'options': {
        'TckrSymb': 'symbol',
        'UndrlygPric': 'underlying',
        'StrkPric': 'strikePrice', 
        'OptnTp': 'optionType',
        'LastPric': 'lastPrice',
        'OpnIntrst': 'openInterest',
        'TLITradgVol': 'volume',
        'HghPric': 'highPrice',
        'LwPric': 'lowPrice',
        'OpnPric': 'openPrice',
        'ClsPric': 'closePrice',
        'SttlmPric': 'settlementPrice'
    }
}

# Trading Strategy Configuration
STRATEGY_CONFIG = {
    'min_price_change_pct': 0.5,           # Minimum price change percentage
    'min_oi_change_pct': 2.0,              # Minimum OI change percentage  
    'min_volume_change_pct': 5.0,          # Minimum volume change percentage
    'high_confidence_threshold': {
        'price_change': 1.0,
        'oi_change': 5.0,
        'volume_change': 10.0
    },
    'oi_ratio_thresholds': {
        'bullish_max': 0.8,                # Max PUT/CALL ratio for bullish
        'bearish_min': 1.2                 # Min PUT/CALL ratio for bearish
    },
    'volume_oi_ratios': {
        'min_volume_oi_ratio': 0.1,        # Minimum volume/OI ratio
        'strong_volume_oi_ratio': 0.3      # Strong volume/OI ratio
    },
    'filters': {
        'min_future_oi': 1000,             # Minimum futures OI
        'min_option_oi': 500,              # Minimum options OI
        'min_volume': 1000,                # Minimum volume
        'min_premium': 1.0                 # Minimum option premium
    }
}

# Analysis Parameters
ANALYSIS_CONFIG = {
    'max_pain_calculation': True,
    'historical_comparison': True, 
    'data_quality_scoring': True,
    'chain_analysis_depth': 20,            # Number of strikes to analyze
    'expiry_preference': 'current',        # 'current' or 'next'
    'min_days_to_expiry': 3                # Avoid last 3 days of expiry
}

# Risk Management
RISK_CONFIG = {
    'max_position_size_pct': 5.0,          # Maximum 5% per trade
    'stop_loss_pct': 25.0,                 # 25% stop loss for options
    'preferred_lot_size': 1,               # Start with 1 lot
    'max_lots_per_trade': 3,               # Maximum 3 lots per trade
    'risk_reward_ratio': 2.0               # Minimum 1:2 risk:reward
}

# Report Settings
REPORT_CONFIG = {
    'enable_enhanced_reports': True,
    'generate_executive_summary': True,
    'generate_trading_dashboard': True,
    'generate_data_quality_report': True,
    'save_historical_data': True,
    'max_opportunities_display': 10
}

# Create required directories on import
def create_directories():
    """Create all required directories"""
    directories = [
        DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR,
        OUT_DIR, SIGNALS_DIR, REPORTS_DIR
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

# Initialize directories
create_directories()
