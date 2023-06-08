"""
class for importing historic exchange rates against EUR
"""
import requests

from app.src.base.processed_response import ProcessedResponse


class ApiCaller:
    BASE_URL = "https://api.exchangerate.host/timeseries"

    def __init__(
            self,
            start_date: str,
            end_date: str,
            exchange_currency: str,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.currency_code = exchange_currency

    def make_call(
            self,
    ) -> ProcessedResponse:
        """
        return a dictionary for exchange rates from start_date to end_date
        :return: dictionary of exchange rates
        """
        url = f"{self.BASE_URL}?base=EUR&start_date={self.start_date}&end_date={self.end_date}&symbols={self.currency_code}"
        response = requests.get(url)
        return ProcessedResponse(
            status_code=response.status_code,
            success=response.ok,
            payload=response.json(),
            start_date=self.start_date,
            end_date=self.end_date,
            exchange_currency=self.currency_code,
        )


if __name__ == "__main__":
    result = ApiCaller(start_date="2021-12-29", end_date="2022-01-01", exchange_currency="USD")
    print(result.make_call())
