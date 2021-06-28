from datetime import datetime
import calendar


class DateManager(object):
    """
        Object which creates a list of dates starting from the initial date and a final date
        Parameters:
        --------------
            start: initial date (default 3 years previous)
            end: today
            
            if the date is included manually it must be of type string in yyyy-mm-dd format.
                example: 1914-06-28 
    """

    def __init__(
        self, start=datetime(datetime.now().year - 3, 1, 1), end=datetime.now()
    ):
        dates = [start, end]
        self.startdate, self.enddate = [
            datetime.strptime(_, "%Y-%m-%d") if isinstance(_, str) else _ for _ in dates
        ]
        self.include_day = True
        self.last_day = True

    def setup_date(self, include_day=True, last_day=True):
        self.include_day = include_day
        self.last_day = last_day
        self.monthlist = []
        total_months = lambda x: x.month + 12 * x.year
        for month in range(
            total_months(self.startdate) - 1, total_months(self.enddate)
        ):
            yr, mth = divmod(month, 12)
            if include_day and last_day:
                last_day = calendar.monthrange(yr, mth + 1)[1]
                self.monthlist.append(
                    datetime(yr, mth + 1, last_day).strftime("%Y-%m-%d")
                )
            elif include_day and not last_day:
                self.monthlist.append(datetime(yr, mth + 1, 1).strftime("%Y-%m-%d"))
            elif not include_day:
                self.monthlist.append(datetime(yr, mth + 1, 1).strftime("%Y-%m"))

    def list_date(self, dateformat="str"):
        self.setup_date(include_day=self.include_day, last_day=self.last_day)
        if dateformat == "str":
            return self.monthlist
        elif dateformat == "date":
            try:
                return [datetime.strptime(x, "%Y-%m-%d") for x in self.monthlist]
            except:
                return [datetime.strptime(x, "%Y-%m") for x in self.monthlist]
        elif dateformat == "str":
            return self.monthlist
        elif dateformat == "int":
            return [x.replace("-", "") for x in self.monthlist]

