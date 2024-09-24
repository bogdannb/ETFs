import boto3
import requests
from typing import List, Tuple, Optional

# Configuration
ETF_SYMBOLS: List[str] = [
    'VGWE.DEX',  # Vanguard FTSE All-World High Dividend Yield UCITS ETF USD Accumulation
    'QDVE.FRK',  # iShares S&P 500 USD Information Technology Sector UCITS ETF (Acc) EUR
    'QDV5.FRK',  # iShares MSCI India UCITS ETF USD Acc
    'VWCE.FRK',  # Vanguard FTSE All-World UCITS ETF USD Accumulation
]
API_KEY: str = '498818a80dmshd82950b4e858a50p1ce907jsn7ece8c59d5a5'
DAYS_THRESHOLD: int = 3
DAYS_TO_DISPLAY: int = 5  # Number of days to display in the email
EMAIL_TO: str = 'bogdan.niculescu@zetta-scale.com'  # Replace with the recipient's email address

sns_client = boto3.client('sns', region_name='eu-west-2')

# The SNS topic ARN (replace with your actual SNS topic ARN)
SNS_TOPIC_ARN: str = 'arn:aws:sns:eu-west-2:730335485390:ETF_Alerts_Topic'


def lambda_handler(event, context) -> None:
    etfs_with_price_down: List[str] = []
    all_etf_price_info: str = ""

    for etf in ETF_SYMBOLS:
        is_descending, last_prices = check_etf_price_down(etf)
        if is_descending:
            etfs_with_price_down.append(etf)
            send_email_alert(etf, last_prices)

        # Add sorted price information (lowest day first) to the main email
        sorted_prices = sorted(last_prices, key=lambda x: x[1])
        all_etf_price_info += f"\n\nETF: {etf}\n"
        all_etf_price_info += "\n".join([f"Date: {date}, Close: {price}" for date, price in sorted_prices])

    subject: str = f"Daily ETF price check - {len(etfs_with_price_down)} ETFs with decreasing prices over the last {DAYS_THRESHOLD} days"
    body_text: str = (f"Found {len(etfs_with_price_down)} ETFs with decreasing prices over the last {DAYS_THRESHOLD} days: {etfs_with_price_down}.\n"
                      f"Here are the closing prices for the last {DAYS_TO_DISPLAY} days for all ETFs (sorted by the lowest day first):\n{all_etf_price_info}\n\n"
                      "Please check the market data for further details.")

    send_email(subject, body_text)


def check_etf_price_down(etf: str) -> Tuple[bool, List[Tuple[str, float]]]:
    # Alpha Vantage URL and parameters
    url: str = "https://www.alphavantage.co/query"
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
        return False, []

    data = response.json()

    # Extract the daily time series data
    time_series = data.get('Time Series (Daily)', {})
    if not time_series:
        print(f"No data retrieved for {etf}")
        return False, []

    # Get the last 5 days of closing prices
    closing_prices: List[Tuple[str, float]] = [
        (key, float(value['4. close'])) for key, value in sorted(time_series.items(), reverse=True)[:DAYS_TO_DISPLAY]
    ]

    if len(closing_prices) < DAYS_THRESHOLD:
        print(f"Not enough data points retrieved for {etf}")
        return False, []

    # Check if the prices for the last 3 days are in descending order
    is_descending: bool = all(closing_prices[i][1] > closing_prices[i + 1][1] for i in range(DAYS_THRESHOLD - 1))

    print(f"{etf} prices for the last {DAYS_TO_DISPLAY} days: {closing_prices}, is_descending: {is_descending}")

    return is_descending, closing_prices


def send_email_alert(etf: str, last_prices: List[Tuple[str, float]]) -> None:
    subject: str = f"Alert: {etf} has been decreasing for {DAYS_THRESHOLD} days"

    # Construct a detailed body text with the last 5 days' prices sorted by lowest day first
    sorted_prices = sorted(last_prices, key=lambda x: x[1])
    body_text: str = (f"The ETF {etf} has been going down for {DAYS_THRESHOLD} consecutive days.\n"
                      f"Here are the last {DAYS_TO_DISPLAY} days of closing prices (sorted by lowest day first):\n\n")

    body_text += "\n".join([f"Date: {date}, Close: {price}" for date, price in sorted_prices])
    body_text += "\n\nPlease check the market data for further details."

    send_email(subject, body_text)


def send_email(subject: str, body_text: str) -> None:
    print(f"Sending email with subject: {subject}")
    response = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=body_text
    )
    print(f"Message sent to SNS topic {SNS_TOPIC_ARN}! Message ID: {response['MessageId']}")


lambda_handler(None, None)
