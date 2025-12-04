# src/is_holiday.py
import datetime
import requests




def is_weekend(date):
return date.weekday() >= 5




def is_nse_holiday(date):
url = "https://www.nseindia.com/api/holiday-master?type=trading"
headers = {"User-Agent": "Mozilla/5.0"}
try:
r = requests.get(url, headers=headers, timeout=10)
data = r.json()
except Exception:
return False


holiday_dates = []
# 'FO' key sometimes holds exchange specific holidays; search recursively
for k, v in data.items():
try:
for item in v:
dt = item.get('tradingDate') or item.get('date')
if dt:
try:
holiday_dates.append(datetime.datetime.strptime(dt, "%d-%b-%Y").date())
except Exception:
pass
except Exception:
pass
return date in holiday_dates




def today_is_market_holiday():
today = datetime.date.today()
if is_weekend(today):
return True
if is_nse_holiday(today):
return True
return False




if __name__ == "__main__":
print('HOLIDAY' if today_is_market_holiday() else 'TRADING_DAY')
