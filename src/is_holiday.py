import datetime
import requests


def is_weekend(date):
    """Check if date is Saturday or Sunday."""
    return date.weekday() >= 5


def is_nse_holiday(date):
    """Check if date is an NSE holiday."""
    url = "https://www.nseindia.com/api/holiday-master?type=trading"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
    except Exception:
        return False

    holiday_dates = []
    
    # Check different possible structures in the response
    for key, value in data.items():
        if isinstance(value, list):
            for item in value:
                # Try to get date from different possible field names
                dt_str = item.get('tradingDate') or item.get('date') or item.get('holidayDate')
                if dt_str:
                    try:
                        # Parse date in format like "02-Oct-2024"
                        dt = datetime.datetime.strptime(dt_str, "%d-%b-%Y").date()
                        holiday_dates.append(dt)
                    except ValueError:
                        try:
                            # Try different format if needed
                            dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d").date()
                            holiday_dates.append(dt)
                        except:
                            pass
    
    return date in holiday_dates


def today_is_market_holiday():
    """Check if today is a market holiday (weekend or NSE holiday)."""
    today = datetime.date.today()
    
    # Check weekend first
    if is_weekend(today):
        return True
    
    # Check NSE holiday
    if is_nse_holiday(today):
        return True
    
    return False


if __name__ == "__main__":
    if today_is_market_holiday():
        print("HOLIDAY")
    else:
        print("TRADING_DAY")
