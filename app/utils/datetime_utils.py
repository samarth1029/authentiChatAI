"""
Datetime util class
returns additional specific epochs (type: datetime.date)
"""

import contextlib
import pytz
from datetime import time, date, timedelta, datetime, timezone
from dateutil.parser import parse


def get_timezone_timestamp_now(timezone: str) -> str:
    """
    return timestamp now for the timezone
    :param timezone:
    :return:
    """
    return datetime.now(tz=pytz.timezone(timezone)).strftime("%Y-%m-%d %H:%M:%S.%f")[
           :-3
           ]


class DatetimeUtils:
    def __init__(self):
        # self.today = datetime.now(timezone.utc).date()
        self.today = datetime.combine(datetime.now(), time.min).replace(tzinfo=pytz.UTC)
        self.yesterday = self.today - timedelta(days=1)
        self.tomorrow = self.today + timedelta(days=1)
        self.first_day_of_this_month = self.today.replace(day=1)
        self.last_day_of_prev_month = self.first_day_of_this_month - timedelta(days=1)
        self.first_day_of_prev_month = self.first_day_of_this_month - timedelta(
            days=self.last_day_of_prev_month.day
        )
        self.seven_days_ago = self.today - timedelta(days=7)
        self.now = datetime.now() - timedelta(minutes=3)
        self.utc_now = datetime.now(timezone.utc) - timedelta(minutes=3)
        self.now_string = self.get_timestamp_string(self.now)

    @staticmethod
    def minus_days(start_date, num_days: int):
        return start_date - timedelta(days=num_days)

    @staticmethod
    def plus_days(start_date, num_days: int):
        return start_date + timedelta(days=num_days)

    def get_default_start_date(self) -> str:
        return self.first_day_of_prev_month.strftime("%Y-%m-%d") + "T00:00:00+00:00"

    def get_default_end_date(self) -> str:
        return self.first_day_of_this_month.strftime("%Y-%m-%d") + "T00:00:00+00:00"

    def get_valid_dates(self, start_date: str = None, end_date: str = None) -> tuple:
        start_date = self.format_date_as_timestamp(start_date) if start_date else None
        end_date = self.format_date_as_timestamp(end_date) if end_date else None
        return (
            start_date or self.get_default_start_date(),
            end_date or self.get_default_end_date(),
        )

    @staticmethod
    def get_timestamp_string(timestamp, string_format: str = "%Y%m%d_%H%M%S%f"):
        return timestamp.strftime(string_format)

    @staticmethod
    def format_date_as_timestamp(date_text: str, timestamp: str = None) -> str:
        """
        formats date_ or datetime_text passed in into datetime string,
        if no timestamp is passed, then time defaults to 00:00:00(+00:00)
        :param date_text: string to be parsed/formatted
        :param timestamp: str
        :return: datetime_str: string in  "%Y-%m-%dT%H:%M:%S" format
        """
        datetime_str = ""
        if date_text and timestamp and "T" not in date_text:
            date_text = f"{date_text}T{timestamp}"
        if date_text and "+" in date_text:
            _tz = f"+{date_text.split('+')[1]}"
            date_text = date_text.split("+")[0]
        else:
            _tz = "+00:00"
        if date_text and parse(date_text):
            _check_format, _plus_text = (
                ("%Y-%m-%dT%H:%M:%S", _tz)
                if date_text and "T" in date_text
                else ("%Y-%m-%d", f"T00:00:00{_tz}")
                if date_text
                else ("", "")
            )
            date_text = datetime.strptime(date_text, _check_format)
            datetime_str = f"{datetime.strftime(date_text, _check_format)}{_plus_text}"
        return datetime_str

    @property
    def phrase_datetime_mapping(self):
        return {
            "${today()}": self.today,
            "${yesterday()}": self.yesterday,
            "${tomorrow()}": self.tomorrow,
            "${now()}": self.now,
            "${utc_now()}": self.utc_now,
        }

    def get_datetime_for_phrase(self, datetime_phrase: str) -> datetime:
        """

        :param datetime_phrase:
        :return:
        """
        _phrase_main = (
            datetime_phrase.split(":")[0] if ":" in datetime_phrase else datetime_phrase
        )
        _phrase_suffix = datetime_phrase.split(":")[1] if ":" in datetime_phrase else ""

        _datetime_main = self.phrase_datetime_mapping.get(_phrase_main)
        if _phrase_suffix and (_operator := "-" if "-" in _phrase_suffix else "+"):
            with contextlib.suppress(Exception):
                if _value := int(_phrase_suffix[_phrase_suffix.rfind(_operator) + 1 :]):
                    if _operator == "+":
                        _datetime_main = self.plus_days(_datetime_main, _value)
                    elif _operator == "-":
                        _datetime_main = self.minus_days(_datetime_main, _value)
        return _datetime_main

    def get_timestamp_for_phrase(self, datetime_phrase: str) -> str:
        """

        :param datetime_phrase:
        :return:
        """
        _datetime = self.get_datetime_for_phrase(datetime_phrase=datetime_phrase)
        return f"{datetime.strftime(_datetime, '%Y-%m-%dT%H:%M:%S')}{'+00:00'}"

    def get_datetime_for_mapping_datetime_dict(self, mapping_datetime: dict) -> dict:
        """

        :param mapping_datetime:
        :return:
        """
        mapping_datetime["date"] = mapping_datetime.get("date", "")
        mapping_datetime["time"] = mapping_datetime.get("time", "")
        for key, value in mapping_datetime.items():
            mapping_datetime[key] = (
                self.get_datetime_for_phrase(value) if "${" in value else value
            )
        if isinstance(mapping_datetime["date"], date):
            mapping_datetime["date"] = mapping_datetime["date"].strftime("%Y-%m-%d")
        if isinstance(mapping_datetime["time"], time):
            mapping_datetime["time"] = mapping_datetime["time"].strftime("%H:%M:%S")
        return mapping_datetime

    @staticmethod
    def format_timestamp_as_date_time(timestamp: str) -> dict:
        """
        split incoming timestamp into date and time
        TODO: what happens if only timestamp such as T00:00:00 is passed
        :param timestamp:
        :return:
        """
        if "T" not in timestamp:
            return {"date": timestamp, "time": ""}
        _date_time_split = timestamp.split("T")
        return {"date": _date_time_split[0], "time": _date_time_split[1]}
