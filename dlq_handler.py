import json
import os
import boto3
from datetime import datetime

from base import Report, Status
from db import update_report_status

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-2")


def lambda_handler(event, context):
    total_records = len(event["Records"])
    print(f"[dlq] Received {total_records} record(s)")
    processed_count = 0

    for idx, record in enumerate(event["Records"]):
        try:
            body = json.loads(record["body"])

            # DLQ messages may be wrapped (original record from main lambda)
            # or raw report messages — handle both
            if "body" in body:
                inner = json.loads(body["body"])
            else:
                inner = body

            report_id = inner["report_id"]
            batch_no = inner["batch_no"]

            prefix = f"[dlq {idx+1}/{total_records} report={report_id} batch={batch_no}]"
            print(f"{prefix} Marking as FAILED")

            update_report_status(report_id, batch_no, Status.FAILED, error_msg="Marked Report as Failed to process and informed concerned stakeholders")

            print(f"{prefix} Done")
            processed_count += 1

        except Exception as e:
            print(f"[dlq {idx+1}/{total_records}] Error processing DLQ message: {str(e)}")

    print(f"[dlq] Processed {processed_count}/{total_records} messages")
    return {"processed_messages": processed_count}
