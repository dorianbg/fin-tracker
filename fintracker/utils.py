import datetime
import duckdb
import pandas as pd
import os
from logging.handlers import TimedRotatingFileHandler
from logging import StreamHandler
import random
import string
import logging.handlers
import logging
from dataclasses import dataclass
from fintracker import consts

_conns: dict[str, duckdb.DuckDBPyConnection] = {}


def add_csv_ext(path):
    return path if path.endswith(consts.csv_ext) else path + consts.csv_ext


def add_pickle_ext(path):
    return path if path.endswith(consts.pickle_ext) else path + consts.pickle_ext


def is_business_day(date):
    return bool(len(pd.bdate_range(date, date)))


@dataclass
class JobDef:
    ticker_full: str
    start_date: datetime.datetime
    end_date: datetime.datetime

    def __post_init__(self):
        start_date = self.start_date
        while not is_business_day(start_date):
            start_date = start_date + datetime.timedelta(days=1)
        self.start_date = start_date
        end_date = self.end_date
        while not is_business_day(end_date - datetime.timedelta(days=1)):
            end_date = end_date - datetime.timedelta(days=1)
        self.end_date = end_date


def get_duckdb_conn(filepath, init_cmd="", **kwargs) -> duckdb.DuckDBPyConnection:
    if filepath in _conns:
        return _conns[filepath]
    else:
        conn = duckdb.connect(database=filepath, **kwargs)
        if init_cmd is not None and init_cmd != "":
            logging.info(f"Init db with {init_cmd}")
            conn.cursor().execute(init_cmd)
        return conn


def insert_df_to_duckdb(
    conn: duckdb.DuckDBPyConnection,
    dataframe: pd.DataFrame,
    table_name: str,
    col_select: str,
    dedup: str = "",
):
    if dataframe is None or len(dataframe) == 0:
        return
    cursor = conn.cursor()
    view_name = "temp_df_" + "".join(
        random.choice(string.ascii_uppercase) for _ in range(10)
    )
    cursor.register(view_name, dataframe)
    stmt = f"insert into {table_name} (select {col_select} from {view_name} {dedup}) "
    logging.info(
        f"Inserting {len(dataframe)} rows to {table_name} using statement: {stmt}"
    )
    cursor.execute(stmt)
    cursor.unregister(view_name)
    cursor.commit()


def missing_timerange(conn: duckdb.DuckDBPyConnection, mark_all_as_missing: bool):
    remove_join = "and 1 = 0" if mark_all_as_missing else ""
    query = f"""
        select
            t.ticker_full as ticker,
            coalesce(h.latest_date, date_trunc('day', get_current_timestamp() at time zone '{consts.timezone}' - interval '{consts.lookback_period}')) + interval '1 day' as start_date,
            date_trunc('day',get_current_timestamp() at time zone '{consts.timezone}')  as end_date
        from ticker_ref t left join (
            select
                date_trunc('day',max("date" at time zone 'Europe/Paris')) as latest_date,
                ticker_full
            from historical_prices h
            group by ticker_full
        ) as h on h.ticker_full = t.ticker_full {remove_join}
    """
    logging.info(query)
    return conn.cursor().execute(query).fetchall()


def setup_logging():
    log_directory = "logs"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    log_file = os.path.join(log_directory, "finance.log")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    LOG_FORMAT = "%(asctime)s|%(levelname)s|%(module)s|%(funcName)s|%(process)s|%(lineno)d|%(message)s"
    formatter = logging.Formatter(LOG_FORMAT)
    # add file log handler
    rotating_file_handler = TimedRotatingFileHandler(
        log_file, when="midnight", backupCount=8
    )
    rotating_file_handler.setLevel(logging.DEBUG)
    rotating_file_handler.setFormatter(formatter)
    root_logger.addHandler(rotating_file_handler)
    # if os.environ.get("RUN_MODE", "dev") != "prod":
    # add stdout logger
    stream_handler = StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)
