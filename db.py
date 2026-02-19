import os
import boto3
from datetime import datetime
from typing import Optional
from boto3.dynamodb.conditions import Key

from base import Report, Status

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-2")
TABLE_NAME = os.environ.get("TABLE_NAME", "reports")

_boto_kwargs = {"region_name": AWS_REGION}

dynamodb = boto3.resource("dynamodb", **_boto_kwargs)
table = dynamodb.Table(TABLE_NAME)


def create_report(report: Report) -> dict:
    item = {
        "report_id": report.report_id,
        "batch_no": report.batch_no,
        "status": report.status.value,
        "s3_key": report.s3_key,
        "payload": report.payload,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }
    table.put_item(Item=item)
    return item



def get_report(report_id: int, batch_no: int) -> Optional[dict]:
    response = table.get_item(Key={"report_id": report_id, "batch_no": batch_no})
    return response.get("Item")


def get_reports_by_batch(batch_no: int) -> list[dict]:
    response = table.query(
        IndexName="batch_no-index",
        KeyConditionExpression=Key("batch_no").eq(batch_no),
    )
    return response.get("Items", [])


def update_report_status(report_id: int, batch_no: int, status: Status, s3_key: Optional[str] = None) -> dict:
    update_expr = "SET #status = :status, updated_at = :updated_at"
    expr_values = {
        ":status": status.value,
        ":updated_at": datetime.now().isoformat(),
    }
    expr_names = {"#status": "status"}

    if s3_key is not None:
        update_expr += ", s3_key = :s3_key"
        expr_values[":s3_key"] = s3_key

    response = table.update_item(
        Key={"report_id": report_id, "batch_no": batch_no},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames=expr_names,
        ReturnValues="ALL_NEW",
    )
    return response["Attributes"]


def delete_report(report_id: int, batch_no: int) -> bool:
    table.delete_item(Key={"report_id": report_id, "batch_no": batch_no})
    return True
