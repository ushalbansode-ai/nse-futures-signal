#!/usr/bin/env python3
"""
Test script to verify artifact downloading
"""

import os
import sys
import pandas as pd
from datetime import datetime

def test_historical_data():
    print("🧪 Testing Historical Data System")
    print("=" * 50)
    
    processed_dir = "data/processed"
    
    # Check if directory exists
    if not os.path.exists(processed_dir):
        print(f"❌ Directory '{processed_dir}' does not exist!")
        print("Please run the workflow first to create artifacts")
        return False
    
    print(f"✅ Directory exists: {processed_dir}")
    
    # List all files
    all_files = os.listdir(processed_dir)
    print(f"\n📁 Files in directory: {len(all_files)} files")
    
    csv_files = [f for f in all_files if f.endswith('.csv')]
    txt_files = [f for f in all_files if f.endswith('.txt')]
    
    print(f"📊 CSV files: {len(csv_files)}")
    print(f"📄 TXT files: {len(txt_files)}")
    
    if csv_files:
        print("\n🔍 CSV File Details:")
        for csv_file in sorted(csv_files):
            filepath = os.path.join(processed_dir, csv_file)
            try:
                # Read just the first row to check structure
                df_sample = pd.read_csv(filepath, nrows=1)
                size_mb = os.path.getsize(filepath) / (1024 * 1024)
                
                print(f"\n  📁 File: {csv_file}")
                print(f"    Size: {size_mb:.2f} MB")
                print(f"    Columns: {len(df_sample.columns)}")
                
                # Check for required columns
                required_cols = ['instrument_type', 'symbol', 'lastPrice']
                missing = [col for col in required_cols if col not in df_sample.columns]
                if missing:
                    print(f"    ⚠️ Missing columns: {missing}")
                else:
                    print(f"    ✅ Required columns present")
                
                # Check date in filename
                if csv_file.startswith('nse_fo_'):
                    date_str = csv_file.replace('nse_fo_', '').replace('.csv', '')
                    print(f"    Date in filename: {date_str}")
                    
            except Exception as e:
                print(f"    ❌ Error reading {csv_file}: {e}")
    
    else:
        print("\n❌ No CSV files found!")
        print("Make sure the workflow is saving data correctly")
        return False
    
    # Check if README exists
    readme_path = os.path.join(processed_dir, "README.txt")
    if os.path.exists(readme_path):
        print(f"\n📋 README.txt exists")
        with open(readme_path, 'r') as f:
            content = f.read()
            print(f"First 3 lines:\n{content.splitlines()[:3]}")
    
    return True

if __name__ == "__main__":
    success = test_historical_data()
    print("\n" + "=" * 50)
    if success:
        print("✅ Test PASSED - Historical data system is working")
    else:
        print("❌ Test FAILED - Check the workflow configuration")
        sys.exit(1)
