from dagster import build_op_context

from analytics.ops.weather import get_cities, transform_weather, CitiesConfig
from analytics.ops.alpaca import transform_alpaca

def test_get_cities():
    assert get_cities(
        context=build_op_context(),
        config= CitiesConfig(city_path="analytics_tests/fixtures/csv/australian_capital_cities.csv")) == [
        "canberra",
        "sydney",
        "darwin"
    ]

def test_transform_weather():
    input_data = [
        {"id": 123, "dt": 123456780, "name": "Perth", "main.temp": 29.5, "some_other_column": "blah"},
        {"id": 124, "dt": 123456781, "name": "Sydney", "main.temp": 14.7, "some_other_column": "blah"},
        {"id": 125, "dt": 123456782, "name": "Melbourne", "main.temp": 20.3, "some_other_column": "blah"}
    ]
    expected_data = [
        {"id": 123, "datetime": 123456780, "name": "Perth", "temperature": 29.5},
        {"id": 124, "datetime": 123456781, "name": "Sydney", "temperature": 14.7},
        {"id": 125, "datetime": 123456782, "name": "Melbourne", "temperature": 20.3}
    ]
    actual_data = transform_weather(context=build_op_context(), data=input_data)
    assert actual_data == expected_data

def test_transform_alpaca():
    input_data = [
        {"stock_ticker": "aapl", "i": 1, "t": "2023-01-01 00:00:00Z", "x": "V", "p": 102.1, "s": 10, "some_other_column": "blah"},
        {"stock_ticker": "aapl", "i": 2, "t": "2023-01-01 00:01:00Z", "x": "V", "p": 104.1, "s": 50, "some_other_column": "blah"},
    ]
    expected_data = [
        {"stock_ticker": "aapl", "id": 1, "timestamp": "2023-01-01 00:00:00Z", "exchange": "V", "price": 102.1, "size": 10},
        {"stock_ticker": "aapl", "id": 2, "timestamp": "2023-01-01 00:01:00Z", "exchange": "V", "price": 104.1, "size": 50},
    ]
    actual_data = transform_alpaca(context=build_op_context(), data=input_data)
    assert actual_data == expected_data
