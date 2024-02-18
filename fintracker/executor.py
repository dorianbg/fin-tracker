import argparse
import logging
import time
import pandas as pd
import yfinance as yf
from fintracker import consts
from fintracker.utils import add_csv_ext, add_pickle_ext, JobDef, get_duckdb_conn, missing_timerange, \
    insert_df_to_duckdb
import os
from pathlib import Path
import traceback
from sqlalchemy import create_engine

__duckdb_conn = None


def get_transformed_df(job: JobDef):
    etf = yf.Ticker(ticker=job.ticker_full)
    etf.tz = "UTC"
    df = etf.history(interval="1d", start=job.start_date.strftime("%Y-%m-%d"),
                     end=job.end_date.strftime("%Y-%m-%d"))
    df = df.reset_index()
    df['ticker'] = job.ticker_full.split(".")[0]
    df['ticker_full'] = job.ticker_full
    df.rename(columns={x: x.lower().replace(" ", "_") for x in df.columns}, inplace=True)
    return df


def upload_data_to_postgres():
    global __duckdb_conn
    dbname = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]

    # Create a connection string
    connection_str = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
    engine = create_engine(connection_str)

    table_name = "latest_performance"
    df = __duckdb_conn.sql(f"select * from {table_name}").df()
    df.to_sql(table_name, engine, index=False, if_exists='replace')
    logging.info(f"Inserted df with shape {df.shape} to {table_name}")


def check_for_dividends(df: pd.DataFrame) -> bool:
    return not df.empty and (df['dividends'] > 0).any()


def check_existing_data(ticker_full: str) -> bool:
    global __duckdb_conn
    res = __duckdb_conn.execute(f"select count(*) as cnt from {consts.hist_prices_table_name} "
                                f"where ticker_full like '%{ticker_full}%'").fetchdf()
    return res['cnt'][0] > 0


def backup_existing_data(ticker_full: str) -> int:
    global __duckdb_conn
    res = __duckdb_conn.execute(f"select * from {consts.hist_prices_table_name} "
                                f"where ticker_full like '%{ticker_full}%'").fetchdf()
    min_date = res["date"].min().strftime("%Y-%m-%d")
    max_date = res["date"].max().strftime("%Y-%m-%d")
    csv_path = add_csv_ext(create_out_path(dir=consts.store_raw_dir, ticker_full=ticker_full, start_date=min_date,
                                           end_date=max_date))
    res.to_csv(csv_path)
    tmp = pd.read_csv(filepath_or_buffer=csv_path)
    assert len(tmp) == len(res)
    __duckdb_conn.execute(f"insert into {consts.dividend_tracker_table_name} values "
                          f"('{ticker_full}', now(), '{csv_path}')")
    return len(tmp)


def delete_existing_data(ticker_full: str) -> bool:
    del_q = (f"delete from {consts.hist_prices_table_name} where ticker_full = '{ticker_full}'")
    global __duckdb_conn
    cur = __duckdb_conn.cursor()
    cur.execute("BEGIN TRANSACTION;")
    cur.execute(del_q)
    cur.execute("COMMIT;")
    return True


def create_out_path(dir: str, ticker_full: str, start_date: str, end_date: str):
    base_temp_path = os.path.join(dir, ticker_full, f"{start_date}_{end_date}")
    return base_temp_path


def execute_job(job: JobDef):
    start_date_str: str = job.start_date.strftime("%Y%m%d")
    end_date_str: str = job.end_date.strftime("%Y%m%d")
    if start_date_str == end_date_str or start_date_str > end_date_str:
        return pd.DataFrame()

    base_temp_path = create_out_path(dir=consts.store_raw_dir, ticker_full=job.ticker_full,
                                     start_date=start_date_str, end_date=end_date_str)
    output_dir = Path(base_temp_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    pickle_path = add_pickle_ext(base_temp_path)
    csv_path = add_csv_ext(base_temp_path)
    if os.path.exists(pickle_path) and os.path.exists(csv_path):
        logging.info(f"Data {pickle_path} already exists")
        return pd.read_pickle(pickle_path)
    # call YF API
    hist: pd.DataFrame = get_transformed_df(job=job)
    # issue with YF adjusting dividends which skews past prices - we then back, delete and re-download the data
    if check_for_dividends(hist):
        logging.info(f"Found dividends for {job}")
        if check_existing_data(ticker_full=job.ticker_full):
            backup_row_count = backup_existing_data(ticker_full=job.ticker_full)
            logging.info(f"Backed up {backup_row_count} rows for {job}")
            if backup_row_count > 0:
                if delete_existing_data(ticker_full=job.ticker_full):
                    logging.info(f"Deleted existing data for {job}")
                    return pd.DataFrame()

    logging.info(
        f"Downloaded {len(hist)} rows for {job.ticker_full} in time range {start_date_str} and {end_date_str}")
    if not hist.empty:
        hist.to_csv(csv_path)
        hist.to_pickle(pickle_path)

    time.sleep(4)
    return hist


def merge_dfs(dfs_to_insert):
    if len(dfs_to_insert) > 0:
        merged_dfs = pd.concat(dfs_to_insert, ignore_index=True)
        if len(merged_dfs) > 0:
            merged_dfs["date"] = pd.to_datetime(merged_dfs["date"], utc=True)
        return merged_dfs
    return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser("fintracker")
    parser.add_argument("--upload_to_postgres", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    __duckdb_conn = get_duckdb_conn(filepath=consts.db_path,
                                    init_cmd=consts.create_table_stmt + consts.settings_init_cmd +
                                             consts.create_instr_ref + consts.consts_perf_view)
    missing_prices = missing_timerange(__duckdb_conn)
    missing_prices_jobs = [JobDef(ticker_full=j[0], start_date=j[1], end_date=j[2]) for j in missing_prices]
    dfs_to_insert = []
    current_job = None
    try:
        for job in missing_prices_jobs:
            current_job = job
            dfs_to_insert.append(execute_job(current_job))
    except Exception as e:
        logging.error(f"Exception {e} with job {current_job} with traceback {traceback.format_exc()}")
    finally:
        merged_dfs = merge_dfs(dfs_to_insert=dfs_to_insert)
        insert_df_to_duckdb(conn=__duckdb_conn, dataframe=merged_dfs, table_name=consts.hist_prices_table_name,
                            col_select=consts.hist_prices_col_select,
                            dedup=f"EXCEPT select {consts.hist_prices_col_select} from {consts.hist_prices_table_name}")
        __duckdb_conn.commit()
        if args.upload_to_postgres:
            upload_data_to_postgres()
        __duckdb_conn.close()
