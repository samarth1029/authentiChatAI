"""
class to abstract processing of response from ExchangeRate API
"""
from app.models.exchange_rate_models import ProcessedExchangeRateData, ExchangeRateData
from dataclasses import dataclass


@dataclass
class ApiResponse:
    start_date: str
    end_date: str
    exchange_currency: str
    status_code: int
    success: bool
    payload: dict


class ProcessedResponse(ApiResponse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.data = self._parse_api_response()
        self.errors = None

    def dict(self):
        return self.__dict__

    def _parse_api_response(
        self,
    ) -> ProcessedExchangeRateData:
        """

        :return:
        """
        # _api_resp = self.api_response
        _imported_data = ProcessedExchangeRateData(
            base_currency="EUR",
            exchange_currency=self.exchange_currency,
            start_date=self.start_date,
            end_date=self.end_date,
        )
        if self.success and self.payload:
            _imported_data.raw_data = ExchangeRateData(**self.payload)
            _imported_data.update_currency_rates()
        else:
            self.errors = self.status_code
        return _imported_data
