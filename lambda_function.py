import json
import os
import boto3
from datetime import datetime, timedelta
from io import BytesIO


import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from .base import Report, Status
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer, Image
from reportlab.platypus import Paragraph
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

import random
import warnings
warnings.filterwarnings('ignore')

SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-2")

sqs_client = boto3.client("sqs", region_name=AWS_REGION)
s3_client = boto3.client("s3", region_name=AWS_REGION)

# Portfolio configuration

def fetch_data(tickers, period='2mo'):
    data = {}
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period=period)
            data[ticker] = hist
        except:
            print(f"Failed to fetch {ticker}")
    return data


def calculate_portfolio_metrics(portfolio, data, benchmark='SPY'):
    results = []
    total_value = 0
    total_cost = 0

    for ticker, shares in portfolio.items():
        if ticker == benchmark or ticker not in data or data[ticker].empty:
            continue

        current_price = data[ticker]['Close'].iloc[-1]
        prev_close = data[ticker]['Close'].iloc[-2]
        day_change = ((current_price - prev_close) / prev_close) * 100

        position_value = current_price * shares

        # Use 30-day ago price as cost basis
        cost_basis_price = data[ticker]['Close'].iloc[-30] if len(data[ticker]) >= 30 else data[ticker]['Close'].iloc[0]
        cost_basis = cost_basis_price * shares

        results.append({
            'ticker': ticker,
            'shares': shares,
            'current_price': current_price,
            'day_change': day_change,
            'position_value': position_value,
            'cost_basis': cost_basis
        })

        total_value += position_value
        total_cost += cost_basis

    return results, total_value, total_cost


def calculate_portfolio_history(portfolio, data, days=30):
    tickers = [t for t in portfolio.keys() if t != 'SPY']

    # Get common date range
    min_length = min([len(data[t]) for t in tickers if t in data])
    days = min(days, min_length)

    portfolio_values = []

    for i in range(-days, 0):
        daily_value = 0
        for ticker in tickers:
            if ticker in data and len(data[ticker]) >= abs(i):
                price = data[ticker]['Close'].iloc[i]
                shares = portfolio[ticker]
                daily_value += price * shares
        portfolio_values.append(daily_value)

    return portfolio_values


def calculate_advanced_metrics(portfolio_values, benchmark_data):
    returns = pd.Series(portfolio_values).pct_change().dropna()

    # Sharpe Ratio (assuming 0% risk-free rate for simplicity)
    sharpe = (returns.mean() / returns.std()) * np.sqrt(252) if returns.std() != 0 else 0

    # Volatility (annualized)
    volatility = returns.std() * np.sqrt(252) * 100

    # Beta vs benchmark
    if benchmark_data is not None and len(benchmark_data) > 0:
        benchmark_returns = benchmark_data['Close'].iloc[-len(portfolio_values):].pct_change().dropna()

        min_len = min(len(returns), len(benchmark_returns))
        returns_aligned = returns.iloc[-min_len:]
        benchmark_aligned = benchmark_returns.iloc[-min_len:]

        if len(returns_aligned) > 1 and len(benchmark_aligned) > 1:
            covariance = np.cov(returns_aligned, benchmark_aligned)[0][1]
            benchmark_variance = np.var(benchmark_aligned)
            beta = covariance / benchmark_variance if benchmark_variance != 0 else 1.0
        else:
            beta = 1.0
    else:
        beta = 1.0

    # Max Drawdown
    cumulative = (1 + returns).cumprod()
    running_max = cumulative.expanding().max()
    drawdown = (cumulative - running_max) / running_max
    max_drawdown = drawdown.min() * 100

    return {
        'sharpe': sharpe,
        'volatility': volatility,
        'beta': beta,
        'max_drawdown': max_drawdown
    }


def create_pie_chart(portfolio_data, width=3.5, height=3.5):
    fig, ax = plt.subplots(figsize=(width, height))

    labels = [h['ticker'] for h in portfolio_data]
    sizes = [h['position_value'] for h in portfolio_data]
    chart_colors = plt.cm.Set3(range(len(labels)))

    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=chart_colors)
    ax.set_title('Allocation by Ticker', fontsize=10, fontweight='bold')

    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=100)
    buf.seek(0)
    plt.close()

    return buf


def create_line_chart(portfolio_history, width=6, height=3):
    fig, ax = plt.subplots(figsize=(width, height))

    dates = pd.date_range(end=datetime.now(), periods=len(portfolio_history), freq='D')
    ax.plot(dates, portfolio_history, linewidth=2, color='#4472C4')
    ax.fill_between(dates, portfolio_history, alpha=0.3, color='#4472C4')
    ax.set_title('30-Day Portfolio Value Trend', fontsize=10, fontweight='bold')
    ax.set_xlabel('Date', fontsize=8)
    ax.set_ylabel('Value ($)', fontsize=8)
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis='both', labelsize=7)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=100)
    buf.seek(0)
    plt.close()

    return buf


def create_pdf_dashboard(portfolio_data, total_value, total_cost, portfolio_history, metrics):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter,
                           rightMargin=0.5*inch, leftMargin=0.5*inch,
                           topMargin=0.5*inch, bottomMargin=0.5*inch)

    elements = []
    styles = getSampleStyleSheet()

    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=6,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )

    subtitle_style = ParagraphStyle(
        'CustomSubtitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.grey,
        spaceAfter=20,
        alignment=TA_CENTER
    )

    section_style = ParagraphStyle(
        'SectionHeader',
        parent=styles['Heading2'],
        fontSize=12,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=8,
        fontName='Helvetica-Bold'
    )

    # Title
    title = Paragraph("Portfolio Dashboard", title_style)
    elements.append(title)

    date_str = datetime.now().strftime('%B %d, %Y')
    subtitle = Paragraph(date_str, subtitle_style)
    elements.append(subtitle)

    # Portfolio Summary Box
    daily_pnl = total_value - sum([h['position_value'] / (1 + h['day_change']/100) for h in portfolio_data])
    overall_return = ((total_value - total_cost) / total_cost) * 100 if total_cost > 0 else 0

    summary_data = [
        ['Total Portfolio Value', f'${total_value:,.2f}'],
        ['Daily P&L', f'${daily_pnl:+,.2f} ({(daily_pnl/total_value)*100:+.2f}%)'],
        ['Overall Return', f'{overall_return:+.2f}%']
    ]

    summary_table = Table(summary_data, colWidths=[2.5*inch, 2*inch])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f0f0f0')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('GRID', (0, 0), (-1, -1), 1, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ROWBACKGROUNDS', (0, 0), (-1, -1), [colors.HexColor('#e8f4f8'), colors.white]),
    ]))

    elements.append(summary_table)
    elements.append(Spacer(1, 0.2*inch))

    # Holdings Table
    holdings_header = Paragraph("Current Holdings", section_style)
    elements.append(holdings_header)

    table_data = [['Ticker', 'Shares', 'Price', 'Day Change', 'Position Value']]

    for holding in portfolio_data:
        table_data.append([
            holding['ticker'],
            f"{holding['shares']:.0f}",
            f"${holding['current_price']:.2f}",
            f"{holding['day_change']:+.2f}%",
            f"${holding['position_value']:,.2f}"
        ])

    holdings_table = Table(table_data, colWidths=[1*inch, 1*inch, 1.2*inch, 1.3*inch, 1.5*inch])

    table_style = [
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4472C4')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9f9f9')]),
    ]

    # Color code day changes
    for i, holding in enumerate(portfolio_data, start=1):
        if holding['day_change'] > 0:
            table_style.append(('BACKGROUND', (3, i), (3, i), colors.HexColor('#d4edda')))
            table_style.append(('TEXTCOLOR', (3, i), (3, i), colors.HexColor('#155724')))
        elif holding['day_change'] < 0:
            table_style.append(('BACKGROUND', (3, i), (3, i), colors.HexColor('#f8d7da')))
            table_style.append(('TEXTCOLOR', (3, i), (3, i), colors.HexColor('#721c24')))

    holdings_table.setStyle(TableStyle(table_style))
    elements.append(holdings_table)
    elements.append(Spacer(1, 0.3*inch))

    # Charts side by side
    pie_buf = create_pie_chart(portfolio_data)
    line_buf = create_line_chart(portfolio_history)

    pie_img = Image(pie_buf, width=3*inch, height=3*inch)
    line_img = Image(line_buf, width=4*inch, height=2*inch)

    chart_data = [[pie_img, line_img]]
    chart_table = Table(chart_data, colWidths=[3.2*inch, 4.2*inch])
    chart_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ]))

    elements.append(chart_table)
    elements.append(Spacer(1, 0.2*inch))

    # Metrics boxes
    metrics_header = Paragraph("Key Metrics", section_style)
    elements.append(metrics_header)

    metrics_data = [
        ['Sharpe Ratio', 'Volatility', 'Beta', 'Max Drawdown', 'Holdings'],
        [
            f"{metrics['sharpe']:.2f}",
            f"{metrics['volatility']:.2f}%",
            f"{metrics['beta']:.2f}",
            f"{metrics['max_drawdown']:.2f}%",
            f"{len(portfolio_data)}"
        ]
    ]

    metrics_table = Table(metrics_data, colWidths=[1.4*inch]*5)
    metrics_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4472C4')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('BACKGROUND', (0, 1), (-1, 1), colors.HexColor('#e8f4f8')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (-1, 1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('FONTSIZE', (0, 1), (-1, 1), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 1), (-1, 1), 12),
        ('BOTTOMPADDING', (0, 1), (-1, 1), 12),
    ]))

    elements.append(metrics_table)

    # Build PDF
    doc.build(elements)
    return buffer.getvalue()


def collect_data_and_generate_report(PORTFOLIO):

    print("Fetching market data...")
    all_tickers = list(PORTFOLIO.keys())
    data = fetch_data(all_tickers)

    print("Calculating portfolio metrics...")
    portfolio_data, total_value, total_cost = calculate_portfolio_metrics(PORTFOLIO, data)

    print("Calculating portfolio history...")
    portfolio_history = calculate_portfolio_history(PORTFOLIO, data, days=30)

    print("Calculating advanced metrics...")
    benchmark_data = data.get('SPY', None)
    metrics = calculate_advanced_metrics(portfolio_history, benchmark_data)

    print("Generating PDF...")
    pdf_content = create_pdf_dashboard(portfolio_data, total_value, total_cost, portfolio_history, metrics)

    return pdf_content


def lambda_handler(event, context):
    processed_count = 0
    reports = []

    for record in event["Records"]:
        body = json.loads(record["body"])
        report = Report(
            report_id=body["id"],
            batch_no=str(body["batch_no"]),
            status=Status.QUEUED,
        )

        try:
            report.status = Status.IN_PROGRESS
            print(f"batch-{report.batch_no} processing report {report.report_id}")

            pdf_content = collect_data_and_generate_report()

            report.status = Status.UPLOAD_STARTED
            s3_key = f"reports/batch-{report.batch_no}/{report.report_id}/portfolio_dashboard_{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf"

            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=pdf_content,
                ContentType="application/pdf",
            )

            report.s3_key = s3_key
            report.status = Status.FINISHED
            print(f"Uploaded report to s3://{S3_BUCKET_NAME}/{s3_key}")
            processed_count += 1

        except Exception as e:
            report.status = Status.FAILED
            print(f"Error processing report {report.report_id}: {str(e)}")

        reports.append(report.model_dump(mode="json"))

    return {"processed_messages": processed_count, "reports": reports}
