import json
import os
import random
import boto3
from datetime import datetime

from base import Report, Status
from db import create_report, update_report_status
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-2")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_KEY", "")

SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")

sqs_client = boto3.client("sqs", region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

PORTFOLIO = {
    'AAPL': 9,
    'MSFT': 13,
    'GOOGL': 4,
    'TSLA': 7,
    'NVDA': 10,
    'SPY': 20,  # Benchmark
    'AMZN': 2,
    'META': 2,
    'BRK.B': 2,
    'AVGO': 2,
    'GOOG': 2,
    'JPM': 2,
    'LLY': 2,
    'V': 2,
    'UNH': 2,
    'XOM': 2,
    'MA': 2,
    'PG': 2,
    'JNJ': 2,
    'HD': 2,
    'COST': 2,
    'ORCL': 2,
    'AMD': 2,
    'BAC': 2,
    'NFLX': 2,
    'CRM': 2,
    'ABT': 2,
    'WMT': 2,
    'TMO': 2,
    'CVX': 2,
    'ACN': 2,
    'KO': 2,
    'AVY': 2,
    'PM': 2,
    'DIS': 2,
    'PFE': 2,
    'INTU': 2,
    'ADBE': 2,
    'TXN': 2,
    'CSCO': 2,
    'NEE': 2,
    'WFC': 2,
    'ABBV': 2,
    'DHR': 2,
    'COP': 2,
    'IBM': 2,
    'QCOM': 2,
    'CAT': 2,
    'RTX': 2,
    'AXP': 2,
    'GS': 2,
    'BLK': 2,
    'AMGN': 2,
    'BX': 2,
    'PLD': 2,
    'LIN': 2,
}


def get_random_portfolio(n=5, benchmark='SPY'):
    stocks = {k: v for k, v in PORTFOLIO.items() if k != benchmark}
    selected_tickers = random.sample(list(stocks.keys()), min(n, len(stocks)))
    portfolio = {ticker: stocks[ticker] for ticker in selected_tickers}
    portfolio[benchmark] = PORTFOLIO[benchmark]
    return portfolio


def send_message(report: Report):
    message_body = json.dumps(report.model_dump(mode="json"))
    sqs_client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=message_body,
    )


def main():
    batch_no = int(datetime.now().strftime('%Y%m%d%H%M%S'))

    for i in range(100):
        portfolio = get_random_portfolio()

        report = Report(
            report_id = i,
            batch_no = batch_no,
            status = Status.CREATED,
            payload = portfolio,
        )

        # 1. Save to DB first
        create_report(report)
        print(f"[DB] Created report {report}")

        # 2. Try sending to SQS, update DB status accordingly
        try:
            send_message(report)
            update_report_status(report.report_id, report.batch_no, Status.QUEUED)
            print(f"[SQS] Sent report {report.report_id} to queue")
        except Exception as e:
            update_report_status(report.report_id, report.batch_no, Status.FAILED, error_msg = "Failed to Send Message to Queue")
            print(f"[SQS] Failed to send report {report.report_id}: {str(e)}")


if __name__ == "__main__":
    main()
