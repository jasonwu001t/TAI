from polygon import RESTClient
import os


class Polygon:
    def __init__(self, api_key=None):
        """
        Initialize the RESTClient with the provided API key.
        If no API key is provided, it will use the POLYGON_API_KEY environment variable.
        """
        if api_key is None:
            api_key = os.getenv('POLYGON_API_KEY')
        self.client = RESTClient(api_key)

    def get_financials(self, ticker, filing_date_gte):
        """
        Retrieve financial data for a given ticker symbol starting from a specified date.

        :param ticker: The stock ticker symbol (e.g., 'AMZN').
        :param filing_date_gte: The start date in 'YYYY-MM-DD' format.
        :return: A list of financial data objects.
        """
        fin_data = []
        for f in self.client.vx.list_stock_financials(ticker=ticker, filing_date_gte=filing_date_gte):
            fin_data.append(f)
        return fin_data

    def get_dividends(self, ticker, ex_dividend_date_gte):
        """
        Retrieve dividend data for a given ticker symbol starting from a specified date.

        :param ticker: The stock ticker symbol (e.g., 'AMZN').
        :param ex_dividend_date_gte: The start date in 'YYYY-MM-DD' format.
        :return: A list of dividend data objects.
        """
        div_data = []
        for d in self.client.reference.dividends(ticker=ticker, ex_dividend_date_gte=ex_dividend_date_gte):
            div_data.append(d)
        return div_data

    def print_basic_eps(self, fin_data):
        """
        Print the company name, filing date, and basic earnings per share for each financial data entry.

        :param fin_data: A list of financial data objects.
        """
        for i in fin_data:
            name = i.company_name
            date = i.filing_date
            res = i.financials.income_statement
            basic_eps = getattr(res, 'basic_earnings_per_share', None)
            value = getattr(basic_eps, 'value', None) if basic_eps else None
            print(f"Company: {name}, Filing Date: {date}, Basic EPS: {value}")


# Example usage:
if __name__ == "__main__":
    # Initialize the handler (API key is read from environment variable)
    handler = Polygon()

    # Fetch financial data for AMZN since 2021-01-01
    financial_data = handler.get_financials(
        ticker='AMZN', filing_date_gte='2021-01-01')

    # Fetch dividend data for AMZN since 2021-01-01
    dividend_data = handler.get_dividends(
        ticker='AMZN', ex_dividend_date_gte='2021-01-01')

    # Print basic earnings per share from the financial data
    handler.print_basic_eps(financial_data)
