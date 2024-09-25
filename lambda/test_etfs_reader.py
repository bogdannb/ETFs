import pytest
from typing import List, Tuple
from etfs_reader import check_descending_prices, calculate_price_changes

def test_check_descending_prices():
    # Sample ETF symbol
    etf = "TEST_ETF"

    # Test case where prices are in descending order after sorting by date
    descending_prices: List[Tuple[str, float]] = [
        ("2024-09-21", 130.0),
        ("2024-09-22", 135.0),
        ("2024-09-23", 150.0),
        ("2024-09-24", 145.0),
        ("2024-09-25", 140.0)
    ]
    assert check_descending_prices(etf, descending_prices, 3) == True

    # Test case where prices are not in descending order
    non_descending_prices: List[Tuple[str, float]] = [
        ("2024-09-21", 135.0),
        ("2024-09-22", 138.0),
        ("2024-09-23", 140.0),
        ("2024-09-24", 145.0),
        ("2024-09-25", 150.0),
    ]
    assert check_descending_prices(etf, non_descending_prices, 3) == False

    # Test case with fewer data points than the threshold
    insufficient_prices: List[Tuple[str, float]] = [
        ("2024-09-24", 145.0),
        ("2024-09-25", 140.0),
    ]
    assert check_descending_prices(etf, insufficient_prices, 3) == False

def test_calculate_price_changes():
    # Test case with a set of sample prices with 3 random decimal places
    prices: List[Tuple[str, float]] = [
        ("2024-09-21", 100.123),
        ("2024-09-22", 105.456),
        ("2024-09-23", 103.789),
        ("2024-09-24", 107.234),
        ("2024-09-25", 110.567),
    ]

    # Expected output with 3 decimal places for prices and 2 for percentage changes
    expected_output = [
        ("2024-09-21", 100.123, 0.000, 0.00),        # First day, no change
        ("2024-09-22", 105.456, 5.333, 5.33),        # Increase by 5.333, which is approximately 5.32%
        ("2024-09-23", 103.789, -1.667, -1.58),      # Decrease by 1.667, which is approximately -1.58%
        ("2024-09-24", 107.234, 3.445, 3.32),        # Increase by 3.445, which is approximately 3.21%
        ("2024-09-25", 110.567, 3.333, 3.11),        # Increase by 3.333, which is approximately 3.10%
    ]

    # Call the function
    result = calculate_price_changes(prices)

    # Check if the result matches the expected output
    assert result == expected_output


# Run the test if executed as a script
if __name__ == "__main__":
    pytest.main()
