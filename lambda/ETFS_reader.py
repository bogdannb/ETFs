import boto3
import requests

# Configuration
ETF_SYMBOLS = [
    'VGWE.DEX', #Vanguard FTSE All-World High Dividend Yield UCITS ETF USD Accumulation
    'QDVE.FRK', #iShares S&P 500 USD Information Technology Sector UCITS ETF (Acc) EUR
    'QDV5.FRK', #iShares MSCI India UCITS ETF USD Acc
    'VWCE.FRK', #Vanguard FTSE All-World UCITS ETF USD Accumulation
]
API_KEY = '498818a80dmshd82950b4e858a50p1ce907jsn7ece8c59d5a5'
DAYS_THRESHOLD = 3
EMAIL_TO = 'bogdan.niculescu@zetta-scale.com'  # Replace with the recipient's email address

sns_client = boto3.client('sns', region_name='eu-west-2')

# The SNS topic ARN (replace with your actual SNS topic ARN)
SNS_TOPIC_ARN = 'arn:aws:sns:eu-west-2:730335485390:ETF_Alerts_Topic'


def lambda_handler(event, context):
    etfs_with_price_down = []
    for etf in ETF_SYMBOLS:
        if check_etf_price_down(etf):
            etfs_with_price_down.append(etf)
            send_email_alert(etf)

    subject = f"Daily ETF price check - {len(etfs_with_price_down)} ETFs with decreasing prices over the last {DAYS_THRESHOLD} days"
    body_text = f"Found {len(etfs_with_price_down)} ETFs with decreasing prices over the last {DAYS_THRESHOLD} days: {etfs_with_price_down} " \
                "Please check the market data for further details."
    send_email(subject, body_text)

def check_etf_price_down(etf: str) -> bool:
    # Alpha Vantage URL and parameters
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": etf,
        "apikey": API_KEY
    }

    print(f"Retrieving data for {etf} from Alpha Vantage")
    response = requests.get(url, params=params)
    print(f"Response for {etf}: {response.status_code}, content: {response.content}")

    if response.status_code != 200:
        print(f"Failed to retrieve data for {etf}")
        return False

    data = response.json()

    # Extract the daily time series data
    time_series = data.get('Time Series (Daily)', {})
    if not time_series:
        print(f"No data retrieved for {etf}")
        return False

    # Get the last N days of closing prices
    closing_prices = [
        float(value['4. close']) for key, value in sorted(time_series.items(), reverse=True)[:DAYS_THRESHOLD]
    ]
    if len(closing_prices) < DAYS_THRESHOLD:
        print(f"Not enough data points retrieved for {etf}")
        return False

    # Check if all prices are in descending order
    is_descending = all(closing_prices[i] > closing_prices[i + 1] for i in range(len(closing_prices) - 1))
    print(f"{etf} prices: {closing_prices}, is_descending: {is_descending}")

    return is_descending

def send_email_alert(etf):
    subject = f"Alert: {etf} has been decreasing for {DAYS_THRESHOLD} days"
    body_text = (f"The ETF {etf} has been going down for {DAYS_THRESHOLD} consecutive days. "
                 "Please check the market data for further details.")
    send_email(subject, body_text)

def send_email(subject:str, body_text: str) -> None:
    print(f"Sending email with subject: {subject}")
    response = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=body_text
    )
    print(f"Message sent to SNS topic {SNS_TOPIC_ARN}! Message ID: {response['MessageId']}")


lambda_handler(None, None)
