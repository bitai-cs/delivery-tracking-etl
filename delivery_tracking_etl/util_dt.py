from delivery_tracking_etl.config_dt import DT_CONFIG
from datetime import datetime, timezone, timedelta
import pytz

def datetime_by_timezone():
    # Getting the current datetime in UTC
    utc_datetime = datetime.now(timezone.utc)
    print(f"Current UTC time: {utc_datetime}")

    # Getting the local timezone
    timezoneFromConfig = pytz.timezone(DT_CONFIG['TIMEZONE'])
    print(f"Local timezone: {timezoneFromConfig}")

    # Converting the UTC datetime to local datetime
    timezone_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(timezoneFromConfig)
    print(f"Current local time: {timezone_datetime}")

    return timezone_datetime

def yesterday_from(dt):
    return dt - timedelta(days=1)
