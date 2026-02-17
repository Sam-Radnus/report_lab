import json
import os
import boto3
from datetime import datetime
from io import BytesIO

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.enums import TA_RIGHT, TA_LEFT

SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-2")

sqs_client = boto3.client("sqs", region_name=AWS_REGION)
s3_client = boto3.client("s3", region_name=AWS_REGION)


def generate_invoice_pdf(billing_data):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    elements = []
    styles = getSampleStyleSheet()
    
    right_style = ParagraphStyle('right', parent=styles['Normal'], alignment=TA_RIGHT)
    
    # Header
    elements.append(Paragraph("<b>INVOICE</b>", styles['Title']))
    elements.append(Spacer(1, 0.2*inch))
    
    header_data = [
        ['Invoice #:', billing_data.get("invoice_number", "N/A")],
        ['Date:', billing_data.get("date", datetime.now().strftime("%Y-%m-%d"))],
        ['Due Date:', billing_data.get("due_date", "N/A")]
    ]
    header_table = Table(header_data, colWidths=[1.5*inch, 4*inch])
    header_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
    ]))
    elements.append(header_table)
    elements.append(Spacer(1, 0.3*inch))
    
    # Customer
    elements.append(Paragraph("<b>Bill To</b>", styles['Heading3']))
    customer = billing_data.get("customer", {})
    elements.append(Paragraph(customer.get("name", "N/A"), styles['Normal']))
    
    address = customer.get("address", "")
    if address:
        for line in address.split("\n"):
            elements.append(Paragraph(line, styles['Normal']))
    
    elements.append(Spacer(1, 0.3*inch))
    
    # Items
    items_data = [['Description', 'Quantity', 'Unit Price', 'Amount']]
    items = billing_data.get("items", [])
    subtotal = 0
    
    for item in items:
        qty = item.get("quantity", 0)
        price = item.get("unit_price", 0)
        amount = qty * price
        subtotal += amount
        
        items_data.append([
            item.get("description", ""),
            str(qty),
            f"${price:.2f}",
            f"${amount:.2f}"
        ])
    
    items_table = Table(items_data, colWidths=[3*inch, 1*inch, 1.2*inch, 1.2*inch])
    items_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
    ]))
    elements.append(items_table)
    elements.append(Spacer(1, 0.3*inch))
    
    # Totals
    tax_rate = billing_data.get("tax_rate", 0)
    tax = subtotal * tax_rate
    total = subtotal + tax
    
    totals_data = [
        ['', '', 'Subtotal:', f'${subtotal:.2f}'],
        ['', '', f'Tax ({tax_rate * 100:.1f}%):', f'${tax:.2f}'],
        ['', '', 'Total:', f'${total:.2f}']
    ]
    
    totals_table = Table(totals_data, colWidths=[3*inch, 1*inch, 1.2*inch, 1.2*inch])
    totals_table.setStyle(TableStyle([
        ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
        ('FONTNAME', (2, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (2, -1), (-1, -1), 12),
        ('LINEABOVE', (2, -1), (-1, -1), 1, colors.black),
    ]))
    elements.append(totals_table)
    
    doc.build(elements)
    return buffer.getvalue()


def lambda_handler(event, context):
    processed_count = 0

    for record in event["Records"]:
        try:
            body = json.loads(record["body"])
            billing_id = body["id"]
            batch_no = body["batch_no"]
            billing_data = body["data"]

            print(f"batch-{batch_no} processing message {billing_id} with data \n {billing_data}")

            pdf_content = generate_invoice_pdf(billing_data)

            invoice_number = body.get(
                "invoice_number",
                f"INV-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            )

            s3_key = f"invoices/batch-{batch_no}/{billing_id}/{invoice_number}.pdf"

            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=pdf_content,
                ContentType="application/pdf",
            )

            processed_count += 1

        except Exception as e:
            print(f"Error processing message: {str(e)}")

    return {"processed_messages": processed_count}