from datetime import datetime
import pytz

NO_tz = pytz.timezone("Europe/OSlo")


def date_utc(d: datetime) -> datetime:
    return d.astimezone(tz=pytz.utc)


def date_tz_no(d: datetime) -> datetime:
    return d.astimezone(tz=NO_tz)


def now_utc() -> datetime:
    t = datetime.now(tz=pytz.utc)
    return t


def now_tz_no() -> datetime:
    t = datetime().now(tz=NO_tz)
    return t
