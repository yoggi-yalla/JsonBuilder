import dateutil.relativedelta
import datetime

def date(s: str):
    return datetime.date.fromisoformat(s)

def period(s: str):
    unit = s[-1]
    n = int(s[:-1])
    if s[-1] == "D":
        return dateutil.relativedelta.relativedelta(days=n)
    elif s[-1] == "W":
        return dateutil.relativedelta.relativedelta(weeks=n)
    elif s[-1] == "M":
        return dateutil.relativedelta.relativedelta(months=n)
    elif s[-1] == "Y":
        return dateutil.relativedelta.relativedelta(years=n)
    else:
         raise Exception("Invalid period unit: {}, valid units are D, W, M, or Y".format(unit))
