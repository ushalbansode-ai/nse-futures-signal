#!/usr/bin/env python3
RAW_DIR = Path('data/raw')
OUT_DIR = Path('data/processed')
OUT_DIR.mkdir(parents=True, exist_ok=True)


# best-effort column map (edit if your bhavcopy uses different names)
COLUMN_MAP = {
# futures
'SYMBOL': ['SYMBOL', 'SYMBOL_NAME'],
'EXPIRY_DT': ['EXPIRY_DT', 'EXPIRY_DATE', 'EXPIRY'],
'INSTRUMENT': ['INSTRUMENT'],
'STRIKE_PRICE': ['STRIKE_PRICE', 'STRIKE'],
'OPTION_TYPE': ['OPTION_TYPE', 'OPTION_TYP', 'INSTRUMENT'],
'LTP': ['LTP', 'LAST_TRADED_PRICE', 'OPT_LTP'],
'OPEN_INT': ['OPEN_INT', 'OI', 'OPEN_INTEREST'],
'VOLUME': ['VOLUME', 'TOTTRDQTY', 'TRADING_VOLUME'],
'CLOSE_PRICE': ['CLOSE', 'CLOSE_PRICE']
}




def find_col(df, candidates):
for c in candidates:
if c in df.columns:
return c
return None




def normalize_frame(df):
# build a normalized DataFrame with a selected minimal set
out = pd.DataFrame()
for k, cand in COLUMN_MAP.items():
col = find_col(df, cand)
if col:
out[k] = df[col]
else:
out[k] = pd.NA
# ensure proper types
out['STRIKE_PRICE'] = pd.to_numeric(out['STRIKE_PRICE'], errors='coerce')
out['OPEN_INT'] = pd.to_numeric(out['OPEN_INT'], errors='coerce')
out['LTP'] = pd.to_numeric(out['LTP'], errors='coerce')
out['VOLUME'] = pd.to_numeric(out['VOLUME'], errors='coerce')
return out




def main():
files = glob.glob(str(RAW_DIR / '*.csv'))
if not files:
print('No CSV files found in data/raw. Drop your bhavcopy CSVs there.')
return
for f in files:
df = pd.read_csv(f, low_memory=False)
norm = normalize_frame(df)
basename = Path(f).stem
out_path = OUT_DIR / f'{basename}.parquet'
norm.to_parquet(out_path, index=False)
print('Wrote', out_path)


if __name__ == '__main__':
main()
