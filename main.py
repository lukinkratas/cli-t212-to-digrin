import datetime as dt
import os
import time
from io import StringIO

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from custom_utils import s3_put_df, s3_put_object, track_args

BUCKET_NAME = 't212-to-digrin'
TICKER_BLACKLIST = [
    'VNTRF',  # due to stock split
    'BRK.A',  # not available in digrin
]


def get_input_dt() -> str:
    default_dt = dt.date.today() - relativedelta(months=1)
    default_dt_str = default_dt.strftime('%Y-%m')

    input_dt_str = input(
        f'Reporting Year Month in "YYYY-mm" format, or confirm default "{default_dt_str}" by ENTER: \n'
    )

    if not input_dt_str:
        input_dt_str = default_dt_str

    return input_dt_str


def get_first_day_of_month(dt: dt.datetime) -> str:
    first_day_of_month = dt.replace(day=1)

    return first_day_of_month.strftime('%Y-%m-%dT%H:%M:%SZ')


def get_first_day_of_next_month(dt: dt.datetime) -> str:
    first_day_of_month = dt.replace(day=1)
    first_day_of_next_month = first_day_of_month + relativedelta(months=1)

    return first_day_of_next_month.strftime('%Y-%m-%dT%H:%M:%SZ')


@track_args
def create_export(start_dt: str, end_dt: str) -> int:
    """
    Spawns T212 csv export process.

    Args:
        start_dt:str - start datetime in string format %Y-%m-%dT%H:%M:%SZ
        end_dt:str - end datetime in string format %Y-%m-%dT%H:%M:%SZ
    """

    url = 'https://live.trading212.com/api/v0/history/exports'

    payload = {
        'dataIncluded': {
            'includeDividends': True,
            'includeInterest': True,
            'includeOrders': True,
            'includeTransactions': True,
        },
        'timeFrom': start_dt,
        'timeTo': end_dt,
    }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': os.getenv('T212_API_KEY'),
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        print(f'{response.status_code=}')

    return response.json().get('reportId')


@track_args
def fetch_reports() -> list[dict]:
    url = 'https://live.trading212.com/api/v0/history/exports'

    headers = {'Authorization': os.getenv('T212_API_KEY')}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f'{response.status_code=}')

    return response.json()


def map_ticker(ticker: str) -> str:
    ticker = str(ticker).strip()

    ticker_map = {
        'VWCE': 'VWCE.DE',
        'VUAA': 'VUAA.DE',
        'SXRV': 'SXRV.DE',
        'ZPRV': 'ZPRV.DE',
        'ZPRX': 'ZPRX.DE',
        'MC': 'MC.PA',
        'ASML': 'ASML.AS',
        'CSPX': 'CSPX.L',
        'EISU': 'EISU.L',
        'IITU': 'IITU.L',
        'IUHC': 'IUHC.L',
        'NDIA': 'NDIA.L',
    }

    return ticker_map.get(ticker, ticker)


@track_args
def transform(df_bytes: bytes) -> pd.DataFrame:
    # Read input CSV
    df = pd.read_csv(StringIO(df_bytes.decode('utf-8')))

    # Filter out blacklisted tickers
    df = df[~df['Ticker'].isin(TICKER_BLACKLIST)]
    df = df[df['Action'].isin(['Market buy', 'Market sell'])]

    # Apply the mapping to the ticker column
    df['Ticker'] = df['Ticker'].apply(map_ticker)

    # convert dtypes
    df = df.convert_dtypes()

    return df


def main():
    load_dotenv(override=True)

    input_dt_str = get_input_dt()  # used later in the naming of csv
    input_dt = dt.datetime.strptime(input_dt_str, '%Y-%m')

    start = get_first_day_of_month(input_dt)
    end = get_first_day_of_next_month(input_dt)

    report_id = create_export(start, end)
    # optimize for too early fetch_reports call -> report still processing
    time.sleep(4)

    while True:
        # reports: list of dicts with keys:
        #   reportId, timeFrom, timeTo, dataIncluded, status, downloadLink
        reports = fetch_reports()

        if not reports:  # too many calls -> fetch_reports returns None
            time.sleep(60)  # limit 1 call per minute
            continue

        # filter report by report_id, start from the last report
        report = next(
            filter(lambda report: report.get('reportId') == report_id, reports[::-1])
        )

        if report.get('status') == 'Finished':
            download_link = report.get('downloadLink')
            break

    response = requests.get(download_link)
    df = response.content

    if response.status_code == 200:
        s3_put_object(bytes=df, bucket=BUCKET_NAME, key=f't212/{input_dt_str}.csv')

        df = transform(df)
        df.to_csv(f'{input_dt_str}.csv')

        s3_put_df(df, bucket=BUCKET_NAME, key=f'digrin/{input_dt_str}.csv')

    else:
        print(f'{response.status_code=}')


if __name__ == '__main__':
    main()
