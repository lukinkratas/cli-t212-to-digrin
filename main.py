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
    'VNTRF',  # due to stock sp  lit
    'BRK.A',  # not available in digrin
]


def map_ticker(ticker):
    if pd.isna(ticker):
        return None

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
def transform(df_bytes):
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


@track_args
def create_export(start_dt: str, end_dt: str):
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

    if response.status_code == 200:
        return response.json().get('reportId')

    print(response.status_code)


@track_args
def fetch_reports():
    url = 'https://live.trading212.com/api/v0/history/exports'

    headers = {'Authorization': os.getenv('T212_API_KEY')}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()

    print(response.status_code)


def main():
    load_dotenv(override=True)

    # python3 main.py --from 2025-02-01 --to 2025-02-28
    # python3 main.py --month 2025-02
    # python3 main.py # ask for use input
    # -d -- download for local donwload?
    # -c --cloud for aws upload or maybe -a --aws?

    default_dt = dt.date.today() - relativedelta(months=1)
    default_dt_str = default_dt.strftime('%Y-%m')

    input_dt_str = input(
        f'Reporting Year Month in "YYYY-mm" format, or confirm default {default_dt_str} by ENTER: \n'
    )

    if not input_dt_str:
        input_dt_str = default_dt_str

    input_dt = dt.datetime.strptime(input_dt_str, '%Y-%m')

    start_dt = input_dt.replace(day=1)
    start_dt_str = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    end_dt = start_dt + relativedelta(months=1)
    end_dt_str = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    print(start_dt_str, end_dt_str)

    report_id = create_export(start_dt_str, end_dt_str)
    # time.sleep(1) # optimize for too early fetch_reports call -> report still processing

    while True:
        reports = fetch_reports()  # list of dicts with keys: reportId, timeFrom, timeTo, dataIncluded, status, downloadLink

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

    if response.status_code == 200:
        s3_put_object(
            bytes=response.content, bucket=BUCKET_NAME, key=f't212/{input_dt_str}.csv'
        )

        df = transform(response.content)

        s3_put_df(df, bucket=BUCKET_NAME, key=f'digrin/{input_dt_str}.csv')

    else:
        print(response.status_code)


if __name__ == '__main__':
    main()
