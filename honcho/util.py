import os
import os.path
import shutil
import logging
import datetime as dt
import gzip
import shutil
import re
import warnings
import json

LOGGER_FORMAT = '%(asctime)s:%(levelname)s:%(filename)s:%(funcName)s:%(lineno)d\n\t%(message)s\n'
LOGGER_FORMAT = '%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOGGER_FORMAT)

DATETIME_WITH_MS_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\.\d{3}$")

def get_logger(name):
    return logging.getLogger(name)

def get_null_logger():
    logger = logging.getLogger('<null>')
    logger.addHandler(logging.NullHandler())
    return logger

def convert_datetime_to_iso_8601_with_z_suffix(dt: dt.datetime) -> str:
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

def transform_to_utc_datetime(dt: dt.datetime) -> dt.datetime:
    return dt.astimezone(tz=timezone.utc)

def json2buffer(value):
    return json.dumps(value).encode()

def buffer2json(buf):
    return json.loads(buf.decode())

def parse_date(txt):
    if txt is None or txt == "":
        return txt
    return dt.datetime.strptime(txt, '%Y-%m-%d').date()

def parse_time(txt):
    if txt is None or txt == "":
        return txt
    return dt.datetime.strptime(txt, '%H:%M:%S').time()

def parse_datetime(txt):
    if txt is None or txt == "":
        return txt
    return dt.datetime.strptime(txt, '%Y-%m-%d %H:%M:%S')

def unparse_date(d):
    if d is None:
        return d
    return d.strftime('%Y-%m-%d')

def unparse_time(d):
    if d is None:
        return d
    return d.strftime('%H:%M:%S')
    
def unparse_datetime(d):
    if d is None:
        return d
    return d.strftime('%Y-%m-%d %H:%M:%S')

def next_n_days(txt, days):
    return str((dt.datetime.strptime(txt, '%Y-%m-%d') + dt.timedelta(days)).date())

def prior_n_days(txt, days):
    return str((dt.datetime.strptime(txt, '%Y-%m-%d') + dt.timedelta(-days)).date())

def next_date(txt):
    return next_n_days(txt, 1)

def prior_date(txt):
    return prior_n_days(txt, 1)

def datetime2tstmp(txt):
    dt1 = parse_datetime(txt)
    return int(dt1.timestamp())

def dates_between(start_dt_txt, end_dt_txt):
    start_dt = parse_date(start_dt_txt)
    end_dt = parse_date(end_dt_txt)
    assert start_dt <= end_dt
    curr_date = start_dt
    result = []
    delta = dt.timedelta(1)
    while curr_date <= end_dt:
        result.append(curr_date)
        curr_date += delta
    return list([ unparse_date(d) for d in result ])
    
def tstmp2datetime(d):
    return dt.datetime.fromtimestamp(d).strftime('%Y-%m-%d %H:%M:%S')
    
def shallow_equality_test(self, other, attrs):
    for attr in attrs:
        if not hasattr(self, attr) or not hasattr(other, attr):
            msg = f"objects {repr(self)} and {repr(other)} compared but one of them lack the attribute {attr}"
            raise ValueError(msg)
        elif getattr(self, attr) != getattr(other, attr):
            return False
    return True

def build_str(self, attrs, indent=True):
    result = []
    result.append("<")
    result.append(type(self).__name__)
    for i,attr in enumerate(attrs):
        value = getattr(self, attr)
        if i > 0:
            if indent:
                txt = f",\n\t{attr}={repr(value)}"
            else:
                txt = f", {attr}={repr(value)}"
        else:
            if indent:
                txt = f"\n\t{attr}={repr(value)}"
            else:
                txt = f" {attr}={repr(value)}"        
        result.append(txt)
    result.append(">")
    return "".join(result)

def now():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
def escape_sql_value(value):
    if value is None:
        return 'NULL'
    elif not isinstance(value, str):
        return str(value)
    else:
        escaped_value = value.strip().replace("'", "''")
        escaped_value = "'" + escaped_value  + "'"
        return escaped_value