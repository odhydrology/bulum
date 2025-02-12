from datetime import datetime, timedelta
import pandas as pd
import numpy as np




def get_date_format(date_str):
    """
    Trial and error approach to determining the date format of a date string.
    """
    for date_fmt in [r'%Y-%m-%d', r'%d/%m/%Y', r'%d/%m/%Y %H:%M', r'%d/%m/%Y %H:%M:%s']:
        try:
            dt = datetime.strptime(date_str, date_fmt)
            return date_fmt
        except ValueError:
            pass
    raise ValueError('Invalid date format for "{}"'.format(date_str))


def standardize_datestring_format(values):
    """
    Converts a list of date strings into a list of date strings in the format YYYY-MM-DD.
    Tested over the range 0001-01-01 to 9999-12-31.
    """
    date_fmt = get_date_format(values[0])
    np_dates = to_np_datetimes64d(values, date_fmt=date_fmt)
    # start_date = datetime.strptime(values[0], date_fmt) #dateutil.parser.parse(values[0], format=fmt)
    # end_date = datetime.strptime(values[-1], date_fmt) + timedelta(days=1)
    # np_datetimes64s = np.arange(start_date, end_date, dtype='datetime64[D]')
    # if (len(np_datetimes64s) != len(values)):
    #     raise Exception(f"ERROR: Expected {len(np_datetimes64s)} dates between {start_date} and {end_date} but found {len(values)}.")
    answer = [str(t) for t in np_dates]
    return answer


def to_np_datetimes64d(values, date_fmt=r'%Y-%m-%d'):
    start_date = datetime.strptime(values[0], date_fmt)
    end_date = datetime.strptime(values[-1], date_fmt) + timedelta(days=1)
    np_dates = np.arange(start_date, end_date, dtype='datetime64[D]') #Assumes the dates are consecutive!
    if (len(np_dates) != len(values)):
        raise Exception(f"ERROR: Expected {len(np_dates)} dates between {start_date} and {end_date} but found {len(values)}.")
    return np_dates


def get_wy(dates, wy_month=7, using_end_year=False):
    """
    Returns water years, as a list of ints, for a given array of dates. Use this to
    add water year info into a pandas DataFrame. 

    The default (using_end_year=False) aligns water years with the primary water 
    allocation at the start of the water year. The alternative (using_end_year==True) 
    follows the convention used for fiscal years whereby water years are labelled 
    based on their end dates. Using the fiscal convention, the 2022 water year is 
    from 2021-07-01 to 2022-06-30 inclusive.
    """
    # Check if the first values is a string
    if isinstance(dates[0], str):
        np_dates = to_np_datetimes64d(dates)
    else:
        #assume dates are datetime
        np_dates = np.array(dates, dtype='datetime64[D]')
    #d.astype('datetime64[Y]').astype(int) + 1970     #<---- this gives the year
    #d.astype('datetime64[M]').astype(int) % 12 + 1   #<---- this gives the month
    # TODO: the below implementation was originally written for pd.Timestamp, not np.datetime64d. It may be possible to simplify it.
    if using_end_year:
        answer = [(d.astype('datetime64[Y]').astype(int) + 1970) if (d.astype('datetime64[M]').astype(int) % 12 + 1) < wy_month else (d.astype('datetime64[Y]').astype(int) + 1970) + 1 for d in np_dates]
    else:
        answer = [(d.astype('datetime64[Y]').astype(int) + 1970) - 1 if (d.astype('datetime64[M]').astype(int) % 12 + 1) < wy_month else (d.astype('datetime64[Y]').astype(int) + 1970) for d in np_dates]        
    return answer


def get_prev_month_end(stringdate):
    year_str = stringdate[:4] #"2021"
    month_str = stringdate[5:7] #"04"    
    day_str = "31" #default which covers most months
    #
    #Go to previous month
    month_int = (int(month_str) - 1)
    if (month_int == 0):
        month_str = "12"
        year_str = f"{(int(year_str) - 1):04d}"
    else:
        month_str = f"{month_int:02d}"    
    #
    #Set the day
    if month_str in ["04", "06", "09", "11"]:        
        day_str = "30"
    elif month_str == "02":
        year = int(year_str)        
        if year % 4 != 0:
            day_str = "28"
        elif year % 100 != 0:
            day_str = "29"
        elif year % 400 != 0:
            day_str = "28"
        else:
            day_str = "29"
    #
    #Return
    return f"{year_str}-{month_str}-{day_str}"


def get_this_month_end(stringdate):
    year_str = stringdate[:4] #"2021"
    month_str = stringdate[5:7] #"04"    
    day_str = "31" #default which covers most months
    if month_str in ["04", "06", "09", "11"]:        
        day_str = "30"
    elif month_str == "02":
        year = int(year_str)
        if year % 4 != 0:
            day_str = "28"
        elif year % 100 != 0:
            day_str = "29"
        elif year % 400 != 0:
            day_str = "28"
        else:
            day_str = "29"
    return f"{year_str}-{month_str}-{day_str}"


def get_next_month_start(stringdate):
    year_str = stringdate[:4] #"2021"
    month_str = stringdate[5:7] #"04"
    day_str = "01"
    if month_str == "12":
        year_str = f"{(int(year_str) + 1):04d}"
        month_str = "01"
    else:
        month_str = f"{(int(month_str) + 1):02d}"
    return f"{year_str}-{month_str}-{day_str}"


def get_year_and_month(v):
    """
    Returns year and month string e.g. "2022_01", as a list for a given array of dates.
    Use this to aggregate by month.
    """
    # Guard against empty dates
    if len(v) == 0:
        return []
    
    # Check if date values are pandas datetimes
    year_month = None
    if np.issubdtype(type(v[0]), str):
        #pull out the YYYY-MM part of the date string
        year_month = [x[:7] for x in v]
    else:
        #assume dates are datetime
        year_month = [d.strftime(r"%Y-%m") for d in v]
    return year_month
    

def get_month(dates):
    """
    Returns month, as a list of ints, for a given array of dates.
    """
    np_dates = to_np_datetimes64d(dates)
    answer = [(d.astype('datetime64[M]').astype(int) % 12 + 1) for d in np_dates]
    return answer


def get_dates(start_date: datetime | str, end_date=None, days=0, years=1, include_end_date=False, str_format=None):
    """
    Generates a list of daily datetime values from a given start date. The length 
    may be defined by an end_date, or a number of days, or a number of years. This 
    function may be useful for working with daily datasets and models.
    """
    # Check if the start_date is a string and convert to datetime
    if isinstance(start_date, str):
        if (str_format == None):
            str_format = get_date_format(start_date)
        start_date = datetime.strptime(start_date, str_format)
        if ((end_date != None) and isinstance(end_date, str)):
            end_date = datetime.strptime(end_date, str_format)
    # Work out how many days we need to generate
    if (days > 0):
        # great, the user has specified the number of days
        pass
    elif (end_date != None):
        # use end_date
        days = (end_date - start_date).days
        days = days + 1 if include_end_date else days
    else:
        # use years
        end_date = datetime(start_date.year + years, start_date.month, start_date.day,
            start_date.hour, start_date.minute, start_date.second, start_date.microsecond)
        days = (end_date - start_date).days
    # Generate the list of dates
    date_list = [start_date + timedelta(days=x) for x in range(days)]
    # Convert to string format if required
    if (str_format != None):
        date_list = [d.strftime(str_format) for d in date_list]
    return date_list


def get_wy_start_date(df:pd.DataFrame, wy_month=7):
    """
    Returns an appropriate water year start date based on data frame dates and the
    water year start month.

    Args:
        df (pd.DataFrame): Dataframe with date as index
        wy_month (int, optional): Water year start month. Defaults to 7.

    Returns:
        datetime: Water year start date.
    """
    # Check if the index is string
    first_date=df.index[0]
    if (isinstance(first_date, str)):
        first_day=int(first_date[8:10]) #0123-56-89
        first_month=int(first_date[5:7])
        first_year=int(first_date[0:4])
    else:
        # Assume the index is datetime
        first_day=first_date.day
        first_month=first_date.month
        first_year=first_date.year

    if (first_month<wy_month):
        #If month is less than wy_month we can start wy this year
        start_month=wy_month
        start_day=1
        start_year=first_year
    elif (first_month==wy_month):
        #If month equal to wy_month check that data starts on first day of month and set year accordingly
        if (first_day>1):
            start_month=wy_month
            start_day=1
            start_year=first_year+1
        else:
            start_month=wy_month
            start_day=1
            start_year=first_year
    else:
        #If month is greater than wy_month we have to start wy next year
        start_month=wy_month
        start_day=1
        start_year=first_year+1
    
    return datetime(start_year,start_month,start_day)

def get_wy_end_date(df:pd.DataFrame, wy_month=7):
    """
    Returns an appropriate water year end date based on data frame dates and the
    water year start month.

    Args:
        df (pd.DataFrame): Dataframe with date as index
        wy_month (int, optional): Water year start month. Defaults to 7.

    Returns:
        datetime: Water year end date.
    """
    # Check if the index is string
    last_date=df.index[(len(df) - 1)]
    if (isinstance(last_date, str)):
        last_day=int(last_date[8:10]) #0123-56-89
        last_month=int(last_date[5:7])
        last_year=int(last_date[0:4])
    else:
        # Assume the index is datetime
        last_day=last_date.day
        last_month=last_date.month
        last_year=last_date.year

    if (wy_month==1):
        wy_month_end=12
    else:
        wy_month_end=wy_month-1

    if wy_month_end in { 1, 3, 5, 7, 8, 10, 12 }:
        wy_day_end=31
    elif wy_month_end in { 4, 6, 9, 11 }:
        wy_day_end=30
    else:
        #Setting number of days in Feb to 28 - handle leap years at the end of this function
        wy_day_end=28

    if (last_month>wy_month_end):
        #If month is greater than wy_month_end we can start wy this year
        end_month=wy_month_end
        end_day=wy_day_end
        end_year=last_year
    elif (last_month==wy_month_end):
        #If month equal to wy_month_end check that data ends on last day of month and set year accordingly
        if (last_day<wy_day_end):
            end_month=wy_month_end
            end_day=wy_day_end
            end_year=last_year-1
        else:
            end_month=wy_month_end
            end_day=wy_day_end
            end_year=last_year
    else:
        #If month is less than wy_month_end we have to end wy last year
        end_month=wy_month_end
        end_day=wy_day_end
        end_year=last_year-1

    #This handles the February's that have 29 days
    if (end_month==2):
        end_day=(datetime(end_year,end_month+1,1) - timedelta(days=1)).day
    
    return datetime(end_year,end_month,end_day)