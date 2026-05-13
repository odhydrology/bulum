import calendar
import warnings
from datetime import datetime, timedelta
from typing import Optional, Union, cast, overload

from collections.abc import Iterable

import numpy as np
import pandas as pd


def get_date_format(date_str: str) -> str:
    """
    Determine the date format of a date string using trial and error.

    This function tries several common date formats and returns the first one
    that successfully parses the input string.

    Parameters
    ----------
    date_str : str
        A date string to analyze for format detection.

    Returns
    -------
    str
        The date format string (e.g., ``'%Y-%m-%d'``, ``'%d/%m/%Y'``) that matches
        the input string.

    Raises
    ------
    ValueError
        If none of the supported date formats can parse the input string.

    Examples
    --------
    >>> get_date_format("2023-12-25")
    '%Y-%m-%d'
    >>> get_date_format("25/12/2023")
    '%d/%m/%Y'
    """
    for date_fmt in [r'%Y-%m-%d', r'%d/%m/%Y', r'%d/%m/%Y %H:%M', r'%d/%m/%Y %H:%M:%s']:
        try:
            _ = datetime.strptime(date_str, date_fmt)
            return date_fmt
        except ValueError:
            pass
    raise ValueError(f'Invalid date format for "{date_str}". '
                     f'Supported formats: {", ".join([r"%Y-%m-%d", r"%d/%m/%Y", r"%d/%m/%Y %H:%M", r"%d/%m/%Y %H:%M:%s"])}')


def standardize_datestring_format(values: list[str]) -> list[str]:
    """
    Converts a list of date strings into a list of date strings in the format YYYY-MM-DD.

    This function automatically detects the input date format and converts all dates
    to the standard ISO 8601 format (YYYY-MM-DD). Uses numpy datetime64 for efficient
    processing. Tested over the range 0001-01-01 to 9999-12-31.

    Parameters
    ----------
    values : list[str]
        List of date strings in any supported format.

    Returns
    -------
    list[str]
        List of date strings in YYYY-MM-DD format.

    Examples
    --------
    >>> standardize_datestring_format(["25/12/2023", "26/12/2023"])
    ['2023-12-25', '2023-12-26']
    """
    date_format = get_date_format(values[0])

    try:
        # Use numpy datetime64 for efficient conversion
        np_dates = to_np_datetimes64d(values, date_fmt=date_format)
    except ValueError as e:
        if "End date format does not match start date format" in str(e):
            # Handle mixed date formats by converting end date to match start format
            end_date_format = get_date_format(values[-1])
            placeholder_values = ["" for _ in values]
            placeholder_values[0] = values[0]
            placeholder_values[-1] = datetime.strptime(values[-1], end_date_format).strftime(date_format)
            np_dates = to_np_datetimes64d(placeholder_values, date_fmt=date_format)
        else:
            raise e

    # Convert numpy datetime64 to clean YYYY-MM-DD strings by taking first 10 characters
    # This strips any timestamp component (e.g., "T00:00:00.000000")
    return [str(t)[:10] for t in np_dates]


def standardise_datestring_format(values):
    """Australian spelling version of :func:`standardize_datestring_format`."""
    return standardize_datestring_format(values)


def to_np_datetimes64d(values: list[str], date_fmt: str = r'%Y-%m-%d',
                       *, check_length: bool = True) -> np.typing.NDArray[np.datetime64]:
    """Convert a list of date strings to numpy datetime64[D] array.

    This function efficiently converts date strings to numpy datetime64 arrays with
    day precision. It generates all dates between the first and last date in the input.
    Handles edge cases at the end of the representable date range (9999-12-31).

    Parameters
    ----------
    values : list[str]
        List of date strings to convert. Can also accept pandas Series.
        Generates all dates from first to last date (inclusive).
    date_fmt : str, default '%Y-%m-%d'
        The date format string for parsing the input dates.
    check_length : bool, default True
        Whether to validate that the number of generated dates matches the input length.
        If True and lengths don't match, issues a warning but still returns all dates
        between start and end. Set to False to suppress the warning.

    Returns
    -------
    np.typing.NDArray[np.datetime64]
        Numpy array of datetime64[D] values from first to last date (inclusive).
        Returns all dates in the range, regardless of input length.

    Warns
    -----
    UserWarning
        If check_length is True and the number of generated dates doesn't match
        the input length, indicating non-consecutive dates or gaps.

    Examples
    --------
    >>> dates = to_np_datetimes64d(['2023-01-01', '2023-01-02', '2023-01-03'])
    >>> dates.dtype
    dtype('<M8[D]')

    >>> len(to_np_datetimes64d(['2023-01-01', '2023-01-03'], check_length=False))
    3
    """
    # Convert pandas Series to list if needed (most robust fix for pandas Series indexing)
    if hasattr(values, 'tolist'):
        values = values.tolist()

    start_date = datetime.strptime(values[0], date_fmt)
    try:
        end_date = datetime.strptime(values[-1], date_fmt)
    except ValueError as e:
        raise ValueError("End date format does not match start date format: "
                         f"{values[0]=} {values[-1]=} {date_fmt=}") from e

    # Handle potential OverflowError at end of representable date range.
    if end_date == datetime(9999, 12, 31):
        np_dates = np.arange(start_date, end_date, dtype='datetime64[D]')
        np_dates = np.append(np_dates, np.datetime64(end_date))
    else:
        np_dates = np.arange(start_date, end_date + timedelta(days=1), dtype='datetime64[D]')

    if check_length and len(np_dates) != len(values):
        warnings.warn(f"Date sequence validation: Expected {len(np_dates)} consecutive dates "
                     f"between {start_date} and {end_date} but found {len(values)} values. "
                     f"This suggests non-consecutive dates or gaps in the sequence. "
                     f"Returning all dates between start and end.",
                     UserWarning, stacklevel=2)
    return np_dates


def get_wy(dates: pd.Index | list[str] | list[np.datetime64], wy_month: int = 7,
           using_end_year: bool = False) -> list[int]:
    """
    Returns water years for a given array of dates.

    Use this function to add water year information into a pandas DataFrame.
    Assumes consecutive dates for efficiency.

    Parameters
    ----------
    dates : Union[pd.Index, list[str], list[np.datetime64]]
        Array of dates. Assumes consecutive dates.
    wy_month : int, default 7
        Water year start month (1=January, 7=July, etc.).
    using_end_year : bool, default False
        Water year labeling convention:

        - ``False`` : Aligns water years with the primary water allocation at the
          start of the water year.
        - ``True`` : Follows the fiscal year convention whereby water years are
          labeled based on their end dates. Using the fiscal convention, the 2022
          water year is from 2021-07-01 to 2022-06-30 inclusive.

    Returns
    -------
    list[int]
        The water years corresponding to the given dates.

    Examples
    --------
    Basic usage with default July start:

    >>> get_wy(['2023-06-30', '2023-07-01'])
    [2022, 2023]

    Using fiscal year convention:

    >>> get_wy(['2023-06-30', '2023-07-01'], using_end_year=True)
    [2023, 2024]

    Integration with pandas for aggregation:

    >>> df.groupby(get_wy(df.index, wy_month=7)).sum().median()
    """
    # Check if the first values is a string
    if isinstance(dates[0], str):
        np_dates = to_np_datetimes64d(dates)
    else:
        # assume dates are datetime
        np_dates = np.array(dates, dtype='datetime64[D]')
    # d.astype('datetime64[Y]').astype(int) + 1970     #<---- this gives the year
    # d.astype('datetime64[M]').astype(int) % 12 + 1   #<---- this gives the month
    # TODO: the below implementation was originally written for pd.Timestamp, not np.datetime64d. It may be possible to simplify it.
    if using_end_year:
        answer = [(d.astype('datetime64[Y]').astype(int) + 1970)
                  if (d.astype('datetime64[M]').astype(int) % 12 + 1) < wy_month
                  else (d.astype('datetime64[Y]').astype(int) + 1970) + 1
                  for d in np_dates]
    else:
        answer = [(d.astype('datetime64[Y]').astype(int) + 1970) - 1
                  if (d.astype('datetime64[M]').astype(int) % 12 + 1) < wy_month
                  else (d.astype('datetime64[Y]').astype(int) + 1970)
                  for d in np_dates]
    return answer


def get_prev_month_end(stringdate: str) -> str:
    """
    Get the last day of the previous month for a given date.

    Parameters
    ----------
    stringdate : str
        Date string in YYYY-MM-DD format.

    Returns
    -------
    str
        Date string in YYYY-MM-DD format representing the last day of the previous month.

    Examples
    --------
    >>> get_prev_month_end("2023-03-15")
    '2023-02-28'
    >>> get_prev_month_end("2024-03-15")  # Leap year
    '2024-02-29'
    """
    year_str = stringdate[:4]  # "2021"
    month_str = stringdate[5:7]  # "04"

    # Go to previous month
    month_int = int(month_str) - 1
    if month_int == 0:
        month_str = "12"
        year_str = f"{(int(year_str) - 1):04d}"
    else:
        month_str = f"{month_int:02d}"

    # Set the day
    if month_str in ["04", "06", "09", "11"]:
        day_str = "30"
    elif month_str == "02":
        year = int(year_str)
        if calendar.isleap(year):
            day_str = "29"
        else:
            day_str = "28"
    else:
        day_str = "31"

    return f"{year_str}-{month_str}-{day_str}"


def get_this_month_end(stringdate: str) -> str:
    """
    Get the last day of the current month for a given date.

    Parameters
    ----------
    stringdate : str
        Date string in YYYY-MM-DD format.

    Returns
    -------
    str
        Date string in YYYY-MM-DD format representing the last day of the current month.

    Examples
    --------
    >>> get_this_month_end("2023-02-15")
    '2023-02-28'
    >>> get_this_month_end("2024-02-15")  # Leap year
    '2024-02-29'
    >>> get_this_month_end("2023-04-15")
    '2023-04-30'
    """
    year_str = stringdate[:4]  # "2021"
    month_str = stringdate[5:7]  # "04"
    day_str = "31"  # default which covers most months
    if month_str in ["04", "06", "09", "11"]:
        day_str = "30"
    elif month_str == "02":
        year = int(year_str)
        if calendar.isleap(year):
            day_str = "29"
        else:
            day_str = "28"
    else:
        day_str = "31"
    return f"{year_str}-{month_str}-{day_str}"


def get_next_month_start(stringdate: str) -> str:
    """
    Get the first day of the next month for a given date.

    Parameters
    ----------
    stringdate : str
        Date string in YYYY-MM-DD format.

    Returns
    -------
    str
        Date string in YYYY-MM-DD format representing the first day of the next month.

    Examples
    --------
    >>> get_next_month_start("2023-02-15")
    '2023-03-01'
    >>> get_next_month_start("2023-12-15")
    '2024-01-01'
    """
    year_str = stringdate[:4]  # "2021"
    month_str = stringdate[5:7]  # "04"
    day_str = "01"
    if month_str == "12":
        year_str = f"{(int(year_str) + 1):04d}"
        month_str = "01"
    else:
        month_str = f"{(int(month_str) + 1):02d}"
    return f"{year_str}-{month_str}-{day_str}"


def get_year_and_month(v: list[str] | list[datetime]) -> list[str]:
    """
    Extract year and month strings from a list of dates.

    Returns year and month strings in YYYY-MM format for aggregation by month.

    Parameters
    ----------
    v : Union[list[str], list[datetime]]
        List of date strings in YYYY-MM-DD format or datetime objects.

    Returns
    -------
    list[str]
        List of year-month strings in YYYY-MM format.

    Examples
    --------
    >>> get_year_and_month(['2023-01-15', '2023-02-20'])
    ['2023-01', '2023-02']

    >>> from datetime import datetime
    >>> get_year_and_month([datetime(2023, 1, 15), datetime(2023, 2, 20)])
    ['2023-01', '2023-02']
    """
    # Guard against empty dates
    if len(v) == 0:
        return []

    # Check if date values are pandas datetimes
    year_month = None
    if np.issubdtype(type(v[0]), str):
        cast(list[str], v)
        # pull out the YYYY-MM part of the date string
        year_month = [x[:7] for x in v]  # type: ignore
    else:
        cast(list[datetime], v)
        # assume dates are datetime
        year_month = [d.strftime(r"%Y-%m") for d in v]  # type: ignore
    return year_month


def get_month(dates: Iterable[str]) -> list[int]:
    """
    Extract month numbers from a list of date strings.

    Parameters
    ----------
    dates : Iterable[str]
        Iterable of date strings in YYYY-MM-DD format. Assumes consecutive dates.

    Returns
    -------
    list[int]
        List of month numbers (1-12) corresponding to the input dates.

    Examples
    --------
    >>> get_month(['2023-01-15', '2023-01-16'])
    [1, 1]
    >>> get_month(['2023-12-31'])
    [12]
    """
    np_dates = to_np_datetimes64d(dates)
    answer = [(d.astype('datetime64[M]').astype(int) % 12 + 1) for d in np_dates]
    return answer


@overload
def get_dates(start_date: str,
              end_date: str | None = None, days: int = 0, years: int = 1,
              include_end_date: bool = False,
              str_format: str | None = None) -> list[str]:
    ...


@overload
def get_dates(start_date: datetime,
              end_date: datetime | None = None, days: int = 0, years: int = 1,
              include_end_date: bool = False,
              str_format: str | None = None) -> list[str] | list[datetime]:
    ...


def get_dates(start_date: datetime | str,
              end_date: datetime | str | None = None, days: int = 0, years: int = 1,
              include_end_date: bool = False,
              str_format: str | None = None) -> list[str] | list[datetime]:
    """
    Generates a list of daily datetime values from a given start date.

    The length may be defined by an end_date, or a number of days, or a number of years.
    This function is useful for working with daily datasets and models. Defaults to 1 year
    after start_date if end_date, days, and years are not specified.

    Parameters
    ----------
    start_date : Union[datetime, str]
        The starting date for the sequence.
    end_date : Optional[Union[datetime, str]], default None
        The ending date for the sequence. If provided, takes precedence over days and years.
    days : int, default 0
        Number of days to generate. If > 0, takes precedence over years parameter.
    years : int, default 1
        Number of years to generate if neither end_date nor days are specified.
    include_end_date : bool, default False
        Whether to include the end_date in the generated sequence.
    str_format : Optional[str], default None
        If provided, returns string dates in this format instead of datetime objects.

    Returns
    -------
    Union[list[str], list[datetime]]
        A list of datetime objects or formatted date strings covering the specified range.

    Raises
    ------
    ValueError
        If years <= 0 when using years parameter for date generation.

    Examples
    --------
    >>> get_dates(datetime(2023, 1, 1), days=3)
    [datetime.datetime(2023, 1, 1, 0, 0), datetime.datetime(2023, 1, 2, 0, 0), datetime.datetime(2023, 1, 3, 0, 0)]

    >>> get_dates('2023-01-01', '2023-01-03', str_format='%Y-%m-%d')
    ['2023-01-01', '2023-01-02']
    """
    # Check if the start_date is a string and convert to datetime
    if isinstance(start_date, str):
        if str_format is None:
            str_format = get_date_format(start_date)
        start_date = datetime.strptime(start_date, str_format)
        if (end_date is not None and isinstance(end_date, str)):
            end_date = datetime.strptime(end_date, str_format)
    # Work out how many days we need to generate
    if days > 0:
        # great, the user has specified the number of days
        pass
    elif end_date is not None:
        # use end_date
        days = (end_date - start_date).days
        days = days + 1 if include_end_date else days
    else:
        # use years
        if years <= 0:
            raise ValueError(f"Invalid years parameter: {years}. "
                             f"Expected a positive integer greater than 0 for date generation. "
                             f"Use end_date parameter if you need to generate dates in the past.")
        end_date = datetime(start_date.year + years, start_date.month, start_date.day,
                            start_date.hour, start_date.minute,
                            start_date.second, start_date.microsecond)
        days = (end_date - start_date).days
    # Generate the list of dates
    date_list = [start_date + timedelta(days=x) for x in range(days)]
    # Convert to string format if required
    if str_format is not None:
        date_list = [d.strftime(str_format) for d in date_list]  # type: ignore
    return date_list


def _parse_date_components(date_value: str | datetime) -> tuple[int, int, int]:
    """
    Extract year, month, day components from a date string or datetime object.

    Parameters
    ----------
    date_value : Union[str, datetime]
        Date as string in YYYY-MM-DD format or datetime object.

    Returns
    -------
    tuple[int, int, int]
        Tuple of (year, month, day) as integers.

    Raises
    ------
    ValueError
        If date_value is a string but not in valid YYYY-MM-DD format.
    TypeError
        If date_value is not a string or datetime object.
    """
    if isinstance(date_value, str):
        if len(date_value) < 10:
            raise ValueError(f"Date string '{date_value}' must be in YYYY-MM-DD format")
        try:
            year = int(date_value[0:4])
            month = int(date_value[5:7])
            day = int(date_value[8:10])
        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid date string format '{date_value}'. Expected YYYY-MM-DD") from e
    elif isinstance(date_value, datetime):
        year = date_value.year
        month = date_value.month
        day = date_value.day
    else:
        raise TypeError(f"Expected str or datetime, got {type(date_value).__name__}. "
                        "Please pass either a date string in YYYY-MM-DD format or a datetime object.")

    return year, month, day


def get_wy_start_date(df: pd.Series | pd.DataFrame, wy_month: int = 7) -> datetime:
    """
    Returns an appropriate water year start date based on data frame dates and the
    water year start month.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with date as index
    wy_month : int, optional
        Water year start month. Defaults to 7.

    Returns:
        datetime: Water year start date.
    """
    first_year, first_month, first_day = _parse_date_components(df.index[0])

    if first_month < wy_month:
        # If month is less than wy_month we can start wy this year
        start_month = wy_month
        start_day = 1
        start_year = first_year
    elif first_month == wy_month:
        # If month equal to wy_month check that data starts on first day of month and set year accordingly
        if first_day > 1:
            start_month = wy_month
            start_day = 1
            start_year = first_year + 1
        else:
            start_month = wy_month
            start_day = 1
            start_year = first_year
    else:
        # If month is greater than wy_month we have to start wy next year
        start_month = wy_month
        start_day = 1
        start_year = first_year + 1

    return datetime(start_year, start_month, start_day)


def get_wy_end_date(df: pd.DataFrame, wy_month: int = 7) -> datetime:
    """
    Returns an appropriate water year end date based on data frame dates and the
    water year start month.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with date as index
    wy_month : int, optional
        Water year start month. Defaults to 7.

    Returns
    -------
    datetime
        Water year end date.
    """
    last_year, last_month, last_day = _parse_date_components(df.index[-1])

    if wy_month == 1:
        wy_month_end = 12
    else:
        wy_month_end = wy_month-1

    if wy_month_end in {1, 3, 5, 7, 8, 10, 12}:
        wy_day_end = 31
    elif wy_month_end in {4, 6, 9, 11}:
        wy_day_end = 30
    else:
        # Setting number of days in Feb to 28 - handle leap years at the end of this function
        wy_day_end = 28

    if last_month > wy_month_end:
        # If month is greater than wy_month_end we can start wy this year
        end_month = wy_month_end
        end_day = wy_day_end
        end_year = last_year
    elif last_month == wy_month_end:
        # If month equal to wy_month_end check that data ends on last day of month and set year accordingly
        if last_day < wy_day_end:
            end_month = wy_month_end
            end_day = wy_day_end
            end_year = last_year - 1
        else:
            end_month = wy_month_end
            end_day = wy_day_end
            end_year = last_year
    else:
        # If month is less than wy_month_end we have to end wy last year
        end_month = wy_month_end
        end_day = wy_day_end
        end_year = last_year - 1

    # This handles the February's that have 29 days
    if end_month == 2:
        end_day = (datetime(end_year, end_month+1, 1) - timedelta(days=1)).day

    return datetime(end_year, end_month, end_day)
