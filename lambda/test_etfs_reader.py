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
    # Test case with a set of sample prices
    prices: List[Tuple[str, float]] = [
        ("2024-09-21", 100.0),
        ("2024-09-22", 105.0),
        ("2024-09-23", 103.0),
        ("2024-09-24", 107.0),
        ("2024-09-25", 110.0),
    ]

    # Expected output
    expected_output = [
        ("2024-09-21", 100.0, 0.0, 0.0),     # First day, no change
        ("2024-09-22", 105.0, 5.0, 5.0),     # Increase by 5.0, which is 5.0%
        ("2024-09-23", 103.0, -2.0, -1.9),   # Decrease by 2.0, which is approximately -1.9%
        ("2024-09-24", 107.0, 4.0, 3.88),    # Increase by 4.0, which is approximately 3.88%
        ("2024-09-25", 110.0, 3.0, 2.8),     # Increase by 3.0, which is approximately 2.8%
    ]

    # Call the function
    result = calculate_price_changes(prices)

    # Check if the result matches the expected output
    assert result == expected_output

# Run the test if executed as a script
if __name__ == "__main__":
    pytest.main()
