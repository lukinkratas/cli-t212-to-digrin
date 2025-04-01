import datetime
from dateutil.relativedelta import relativedelta
import os
import time
from io import StringIO

import pandas as pd
import requests
from dotenv import load_dotenv

from custom_utils import s3_put_df, s3_put_object, track_args

BUCKET_NAME = 't212-to-digrin'
TICKER_BLACKLIST = [
    'VNTRF',  # due to stock split
    'BRK.A',  # not available in digrin
]


def get_input_dt() -> str:
    today_dt = datetime.date.today()
    default_dt = today_dt - relativedelta(months=1)
    default_dt_str = default_dt.strftime('%Y-%m')

    print('Reporting Year Month in "YYYY-mm" format:')
    print(f'Or confirm default "{default_dt_str}" by ENTER.')
    input_dt_str = input()

    if not input_dt_str:
        input_dt_str = default_dt_str

    return input_dt_str


def get_first_day_of_month(dt: datetime.datetime) -> datetime.datetime:
    return dt.replace(day=1)


def get_first_day_of_next_month(dt: datetime.datetime) -> datetime.datetime:
    next_month_dt = dt + relativedelta(months=1) # works even for Jan and Dec
    return next_month_dt.replace(day=1)


@track_args
def create_export(from_dt: datetime.datetime, to_dt: datetime.datetime) -> int:
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
        'timeFrom': from_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'timeTo': to_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
    }

    headers = {
        'Content-Type': 'application/json',
        'Authorization': os.getenv('T212_API_KEY'),
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        print(f'{response.status_code=}')
        return None

    return response.json().get('reportId')


@track_args
def fetch_reports() -> list[dict]:
    url = 'https://live.trading212.com/api/v0/history/exports'

    headers = {'Authorization': os.getenv('T212_API_KEY')}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f'{response.status_code=}')
        return None

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
    report_df = pd.read_csv(StringIO(df_bytes.decode('utf-8')))

    # Filter out blacklisted tickers
    report_df = report_df[~report_df['Ticker'].isin(TICKER_BLACKLIST)]
    report_df = report_df[report_df['Action'].isin(['Market buy', 'Market sell'])]

    # Apply the mapping to the ticker column
    report_df['Ticker'] = report_df['Ticker'].apply(map_ticker)

    # convert dtypes
    return report_df.convert_dtypes()


def main():
    load_dotenv(override=True)

    input_dt_str = get_input_dt()  # used later in the naming of csv
    input_dt = datetime.datetime.strptime(input_dt_str, '%Y-%m')

    from_dt = get_first_day_of_month(input_dt)
    to_dt = get_first_day_of_next_month(input_dt)

    while True:
        report_id = create_export(from_dt, to_dt)

        if report_id:
            break

        # limit 1 call per 30s
        time.sleep(30)

    # optimize for too early fetch_reports call -> report still processing
    time.sleep(10)

    while True:
        # reports: list of dicts with keys:
        #   reportId, timeFrom, timeTo, dataIncluded, status, downloadLink
        reports = fetch_reports()

        # too many calls -> fetch_reports returns None
        if not reports:
            # limit 1 call per 1min
            time.sleep(60)
            continue

        # filter report by report_id, start from the last report
        report = next(
            filter(lambda report: report.get('reportId') == report_id, reports[::-1])
        )

        if report.get('status') == 'Finished':
            download_link = report.get('downloadLink')
            break

    response = requests.get(download_link)
    t212_df = response.content

    if response.status_code != 200:
        print(f'{response.status_code=}')
        return

    s3_put_object(bytes=t212_df, bucket=BUCKET_NAME, key=f't212/{input_dt_str}.csv')

    digrin_df = transform(t212_df)
    digrin_df.to_csv(f'{input_dt_str}.csv')

    s3_put_df(digrin_df, bucket=BUCKET_NAME, key=f'digrin/{input_dt_str}.csv')


if __name__ == '__main__':
    main()
