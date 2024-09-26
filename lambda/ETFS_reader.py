import boto3
import requests
from typing import List, Tuple

# Configuration
ETF_SYMBOLS = {
    'VGWE.DEX': 'Vanguard FTSE All-World High Dividend Yield UCITS ETF USD Accumulation',
    'QDVE.FRK': 'iShares S&P 500 USD Information Technology Sector UCITS ETF (Acc) EUR',
    'QDV5.FRK': 'iShares MSCI India UCITS ETF USD Acc',
    'VWCE.FRK': 'Vanguard FTSE All-World UCITS ETF USD Accumulation',
    'EUNL.DEX': 'iShares Core MSCI World UCITS ETF USD (Acc)',
    'SXR8.DEX': 'iShares Core S&P 500 UCITS ETF USD (Acc)'
}
API_KEY: str = '498818a80dmshd82950b4e858a50p1ce907jsn7ece8c59d5a5'
DAYS_THRESHOLD: int = 3
DAYS_TO_DISPLAY: int = 10  # Number of days to display in the email
EMAIL_TO: str = 'bogdan.niculescu@zetta-scale.com'  # Replace with the recipient's email address

sns_client = boto3.client('sns', region_name='eu-west-2')

# The SNS topic ARN (replace with your actual SNS topic ARN)
SNS_TOPIC_ARN: str = 'arn:aws:sns:eu-west-2:730335485390:ETF_Alerts_Topic'


def lambda_handler(event, context) -> None:
    etfs_with_price_down: List[str] = []
    all_etf_price_info: str = ""

    for etf, etf_name in ETF_SYMBOLS.items():
        closing_prices = get_closing_sorted_prices(etf)
        price_info_with_changes = calculate_price_changes(closing_prices)

        is_descending = check_descending_prices(etf, closing_prices, DAYS_THRESHOLD)
        if is_descending:
            etfs_with_price_down.append(etf_name)

        all_etf_price_info += f"\n\nETF: {etf_name} ({etf})\n"
        all_etf_price_info += "\n".join([f"Date: {date}, Close: {price:.3f}, Change: {change:.3f} ({percent_change:.2f}%)"
                                         for date, price, change, percent_change in price_info_with_changes])

    subject: str = f"Daily ETF price check - {len(etfs_with_price_down)} ETFs with decreasing prices over the last {DAYS_THRESHOLD} days"
    body_text: str = (f"Found {len(etfs_with_price_down)} ETFs with decreasing prices over the last {DAYS_THRESHOLD} days:\n" +
                      "\n".join(etfs_with_price_down) +
                      f"\n\nHere are the closing prices for the last {DAYS_TO_DISPLAY} days for all ETFs (sorted by earliest date first):\n{all_etf_price_info}\n\n"
                      "Please check the market data for further details.")

    send_email(subject, body_text)


def get_closing_sorted_prices(etf: str) -> List[Tuple[str, float]]:
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
        return []

    data = response.json()

    # Extract the daily time series data
    time_series = data.get('Time Series (Daily)', {})
    if not time_series:
        print(f"No data retrieved for {etf}")
        return []

    closing_prices: List[Tuple[str, float]] = [
        (key, float(value['4. close'])) for key, value in sorted(time_series.items(), reverse=True)[:DAYS_TO_DISPLAY]
    ]

    if len(closing_prices) < DAYS_THRESHOLD:
        print(f"Not enough data points retrieved for {etf}")
        return []

    sorted_prices = sorted(closing_prices, key=lambda x: x[0])
    print(f"{etf} prices for the last {DAYS_TO_DISPLAY} days: {sorted_prices}")

    return sorted_prices


def check_descending_prices(etf: str, closing_prices: List[Tuple[str, float]], days_threshold: int) -> bool:
    if len(closing_prices) < days_threshold:
        print(f"Not enough data points for the last {days_threshold} days")
        return False

    # Sort prices by date in ascending order
    closing_prices_sorted = sorted(closing_prices, key=lambda x: x[0])

    # Check if the last `days_threshold` prices are in descending order
    last_x_prices = closing_prices_sorted[-days_threshold:]
    is_descending = all(last_x_prices[i][1] > last_x_prices[i + 1][1] for i in range(len(last_x_prices) - 1))

    print(f"ETF: {etf}, Last {days_threshold} days' sorted prices: {last_x_prices}, is_descending: {is_descending}")
    return is_descending



def calculate_price_changes(prices: List[Tuple[str, float]]) -> List[Tuple[str, float, float, float]]:
    """
    Calculates the daily change in price and percentage change relative to the previous day.
    """
    price_info_with_changes = []

    for i in range(len(prices)):
        date, price = prices[i]
        if i == 0:
            change = 0.0
            percent_change = 0.0
        else:
            previous_price = prices[i - 1][1]
            change = price - previous_price
            percent_change = (change / previous_price) * 100

        price_info_with_changes.append((date, price, round(change, 3), round(percent_change, 2)))

    return price_info_with_changes


def send_email(subject: str, body_text: str) -> None:
    print(f"Sending email with subject: {subject}")
    response = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=body_text
    )
    print(f"Message sent to SNS topic {SNS_TOPIC_ARN}! Message ID: {response['MessageId']}")


# lambda_handler(None, None)
