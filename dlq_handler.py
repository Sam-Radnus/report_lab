import json
import os

from models import Status
from repository import update_report_status
from logger import get_logger

logger = get_logger("dlq_handler")

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-2")


def lambda_handler(event, context):
    total_records = len(event["Records"])
    logger.info("Received DLQ records", total_records=total_records)
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

            log = logger.bind(report_id=report_id, batch_no=batch_no, index=idx + 1, total=total_records)
            log.info("Marking as FAILED")

            update_report_status(report_id, batch_no, Status.FAILED, error_msg="Marked Report as Failed to process and informed concerned stakeholders")

            log.info("Done")
            processed_count += 1

        except Exception as e:
            logger.error("Error processing DLQ message", index=idx + 1, total=total_records, error=str(e))

    logger.info("DLQ processing complete", processed_count=processed_count, total_records=total_records)
    return {"processed_messages": processed_count}
