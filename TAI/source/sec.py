import requests
import json
from datetime import datetime, timedelta
import yfinance as yf  # Used for fetching stock prices and market data


class SEC:
    def __init__(self, user_agent):
        """
        Initialize the SEC class with a user agent string.

        Args:
            user_agent (str): The User-Agent string to be used in HTTP headers.
        """
        self.user_agent = user_agent
        self.headers = {'User-Agent': self.user_agent}
        self.all_metrics = {
            'shares_outstanding': 'CommonStockSharesOutstanding',
            'eps_basic': 'EarningsPerShareBasic',
            'eps_diluted': 'EarningsPerShareDiluted',
            'dividends_per_share': 'CommonStockDividendsPerShareDeclared',
            'revenue': 'Revenues',
            'net_income': 'NetIncomeLoss',
            'total_assets': 'Assets',
            'total_liabilities': 'Liabilities',
            'equity': 'StockholdersEquity',
            'cash_and_cash_equivalents': 'CashAndCashEquivalentsAtCarryingValue',
            'operating_cash_flow': 'NetCashProvidedByUsedInOperatingActivities',
            'capital_expenditures': 'PaymentsToAcquirePropertyPlantAndEquipment',
            'gross_profit': 'GrossProfit',
            'operating_income': 'OperatingIncomeLoss',
            'research_and_development_expense': 'ResearchAndDevelopmentExpense'
        }

    def get_cik_from_ticker(self, ticker):
        """
        Retrieve the Central Index Key (CIK) for a given ticker symbol.
        """
        url = 'https://www.sec.gov/include/ticker.txt'
        response = requests.get(url, headers=self.headers)
        content = response.text
        lines = content.strip().split('\n')
        ticker_to_cik = {}
        for line in lines:
            ticker_line, cik_line = line.split()
            ticker_to_cik[ticker_line.lower()] = cik_line
        cik = ticker_to_cik.get(ticker.lower())
        if cik:
            cik = cik.zfill(10)
        return cik

    def get_company_facts(self, cik):
        """
        Fetch company facts data from the SEC's EDGAR API.
        """
        url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
        response = requests.get(url, headers=self.headers)
        data = response.json()
        return data

    def get_general_info(self, data, user_ticker):
        """
        Extract general information from the company facts data.
        """
        general_info = {
            'ticker': user_ticker.upper(),
            'company_name': data.get('entityName', 'N/A'),
            'cik': str(data.get('cik', 'N/A')).zfill(10)
        }
        return general_info

    def extract_metric(self, us_gaap, metric_name, start_date, end_date):
        """
        Extract a specific financial metric from the us-gaap data.
        """
        metric = us_gaap.get(metric_name, {})
        if not metric:
            return []
        data_list = []
        units = metric.get('units', {})
        for unit in units:
            for item in units[unit]:
                end_date_str = item.get('end', 'N/A')
                start_date_str = item.get('start', 'N/A')
                if end_date_str == 'N/A' or start_date_str == 'N/A':
                    continue
                try:
                    item_end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                    item_start_date = datetime.strptime(
                        start_date_str, '%Y-%m-%d')
                except ValueError:
                    continue
                if start_date and item_end_date < start_date:
                    continue
                if end_date and item_end_date > end_date:
                    continue
                # Determine period length
                period_days = (item_end_date - item_start_date).days
                if period_days <= 95:
                    period_type = 'Quarterly'
                elif period_days <= 185:
                    period_type = 'Half-Year'
                elif period_days <= 280:
                    period_type = 'Year-to-Date'
                else:
                    period_type = 'Annual'
                data_point = {
                    'start': item.get('start', 'N/A'),
                    'end': item.get('end', 'N/A'),
                    'value': item.get('val', 'N/A'),
                    'unit': unit,
                    'accn': item.get('accn', 'N/A'),
                    'fy': item.get('fy', 'N/A'),
                    'fp': item.get('fp', 'N/A'),
                    'form': item.get('form', 'N/A'),
                    'filed': item.get('filed', 'N/A'),
                    'period_type': period_type
                }
                data_list.append(data_point)
        return data_list

    def get_financial_data(self, ticker, start_date=None, end_date=None, metrics_to_retrieve=None):
        """
        Retrieve historical financial data for a given stock ticker within a date range.

        Args:
            ticker (str): The stock ticker symbol.
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.
            metrics_to_retrieve (list): List of financial data keys to retrieve.
                                        If None, all available data is retrieved.

        Returns:
            dict: A dictionary containing the financial data and general information.
        """
        date_format = "%Y-%m-%d"
        if start_date:
            start_date = datetime.strptime(start_date, date_format)
        if end_date:
            end_date = datetime.strptime(end_date, date_format)

        cik = self.get_cik_from_ticker(ticker)
        if not cik:
            print(f"CIK not found for ticker {ticker}")
            return {}
        data = self.get_company_facts(cik)
        facts = data.get('facts', {})
        us_gaap = facts.get('us-gaap', {})

        general_info = self.get_general_info(data, ticker)

        # If metrics_to_retrieve is None, retrieve all metrics
        if metrics_to_retrieve is None:
            metrics_to_retrieve = list(self.all_metrics.keys())

        financial_data = {
            'general_info': general_info
        }

        # Extract only the requested metrics
        for data_key in metrics_to_retrieve:
            if data_key in self.all_metrics:
                metric_name = self.all_metrics[data_key]
                financial_data[data_key] = self.extract_metric(
                    us_gaap, metric_name, start_date, end_date)
            else:
                print(f"Metric '{data_key}' is not recognized.")

        return financial_data


class FundamentalAnalysis:
    def __init__(self, sec_instance, ticker):
        """
        Initialize the FundamentalAnalysis with an SEC instance and a ticker symbol.

        Args:
            sec_instance (SEC): An instance of the SEC class.
            ticker (str): The stock ticker symbol.
        """
        self.sec = sec_instance
        self.ticker = ticker.upper()
        self.financial_data = None
        self.stock_data = None

    def fetch_financial_data(self, start_date=None, end_date=None, metrics_to_retrieve=None):
        """
        Fetch financial data using the SEC instance.

        Args:
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.
            metrics_to_retrieve (list): List of financial data keys to retrieve.
        """
        self.financial_data = self.sec.get_financial_data(
            self.ticker,
            start_date=start_date,
            end_date=end_date,
            metrics_to_retrieve=metrics_to_retrieve
        )

    def fetch_stock_data(self, start_date=None, end_date=None):
        """
        Fetch historical stock data using yfinance.

        Args:
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.
        """
        stock = yf.Ticker(self.ticker)
        if start_date and end_date:
            self.stock_data = stock.history(start=start_date, end=end_date)
        else:
            self.stock_data = stock.history(period='max')

    def get_latest_stock_price(self):
        """
        Get the latest stock price.

        Returns:
            float: The latest closing stock price.
        """
        if self.stock_data is None:
            self.fetch_stock_data()
        if not self.stock_data.empty:
            latest_price = self.stock_data['Close'][-1]
            return latest_price
        else:
            print("Stock data is empty.")
            return None

    def calculate_market_cap(self):
        """
        Calculate the market capitalization.

        Returns:
            dict: A dictionary containing the market cap and related information.
        """
        # Get the latest stock price
        latest_stock_price = self.get_latest_stock_price()
        if latest_stock_price is None:
            print("Cannot retrieve latest stock price.")
            return {}

        # Try to get shares outstanding from yfinance
        stock = yf.Ticker(self.ticker)
        shares_outstanding = stock.info.get('sharesOutstanding')

        if shares_outstanding:
            # Shares outstanding obtained from yfinance
            pass
        else:
            # Fallback to SEC data
            print(
                "Unable to fetch shares outstanding from yfinance. Falling back to SEC data.")
            shares_data = self.financial_data.get('shares_outstanding', [])
            if not shares_data:
                print("Shares outstanding data not available.")
                return {}

            # Get the latest shares outstanding value
            latest_shares = sorted(
                shares_data,
                key=lambda x: datetime.strptime(x['end'], '%Y-%m-%d')
            )[-1]
            shares_outstanding = float(latest_shares['value'])

            # Adjust shares if unit is not 'shares'
            unit = latest_shares['unit']
            # Map units to multipliers
            unit_multipliers = {
                'shares': 1,
                'iso4217:USD': 1,  # Assuming value is in number of shares, even if unit is USD
                'pure': 1,  # 'pure' unit might represent raw numbers
                # Add more units if necessary
            }

            # Try to get the multiplier, default to 1 if unit is not recognized
            multiplier = unit_multipliers.get(unit.lower(), 1)

            # Apply the multiplier
            shares_outstanding *= multiplier

        market_cap = latest_stock_price * shares_outstanding

        return {
            'latest_stock_price': latest_stock_price,
            'shares_outstanding': shares_outstanding,
            'market_cap': market_cap
        }

    def calculate_pe_ttm(self):
        """
        Calculate PE TTM (Trailing Twelve Months) ratio based on the latest stock price.

        Returns:
            dict: A dictionary containing the PE TTM ratio and related information.
        """
        # Ensure financial data is fetched
        if not self.financial_data:
            self.fetch_financial_data()

        # Get the latest stock price
        latest_stock_price = self.get_latest_stock_price()
        if latest_stock_price is None:
            print("Cannot retrieve latest stock price.")
            return {}

        # Get EPS data
        eps_data = self.financial_data.get('eps_diluted', [])
        if not eps_data:
            print("EPS diluted data not available.")
            return {}

        # Filter Quarterly EPS data
        eps_quarterly = [item for item in eps_data if item.get(
            'period_type') == 'Quarterly']

        # Sort EPS data by end date
        eps_quarterly.sort(
            key=lambda x: datetime.strptime(x['end'], '%Y-%m-%d'))

        # Need at least 4 quarters to calculate TTM EPS
        if len(eps_quarterly) < 4:
            print("Not enough quarterly EPS data to calculate PE TTM.")
            return {}

        # Get the last four quarters
        last_four_quarters = eps_quarterly[-4:]
        eps_ttm = sum(float(item['value']) for item in last_four_quarters)

        if eps_ttm == 0:
            print("EPS TTM is zero, cannot calculate PE TTM.")
            return {}

        pe_ttm = latest_stock_price / eps_ttm

        return {
            'latest_stock_price': latest_stock_price,
            'eps_ttm': eps_ttm,
            'pe_ttm': pe_ttm,
            'as_of_date': datetime.now().strftime('%Y-%m-%d'),
            'quarters': [{
                'fy': item.get('fy', 'N/A'),
                'fp': item.get('fp', 'N/A'),
                'end_date': item['end'],
                'eps_diluted': item['value']
            } for item in last_four_quarters]
        }

    def calculate_pe_ratios_over_time(self, period_type='Quarterly'):
        """
        Calculate historical PE ratios based on EPS data over time.

        Args:
            period_type (str): The period type ('Quarterly' or 'Annual').

        Returns:
            list: A list of dictionaries containing PE ratios over time.
        """
        # Ensure financial data is fetched
        if not self.financial_data:
            self.fetch_financial_data()

        # Get EPS data
        eps_data = self.financial_data.get('eps_diluted', [])
        if not eps_data:
            print("EPS diluted data not available.")
            return []

        # Filter EPS data based on period_type
        eps_filtered = [item for item in eps_data if item.get(
            'period_type') == period_type]

        if not eps_filtered:
            print(f"No EPS data available for period type: {period_type}")
            return []

        # Get unique end dates
        date_list = sorted({item['end'] for item in eps_filtered})

        # Convert date_list to datetime objects
        date_list_dt = [datetime.strptime(
            date_str, '%Y-%m-%d') for date_str in date_list]

        # Remove future dates
        date_list_dt = [
            date for date in date_list_dt if date <= datetime.now()]

        if not date_list_dt:
            print("No valid dates to process.")
            return []

        # Fetch stock data for the required date range
        # Subtract extra days to handle weekends/holidays
        min_date = min(date_list_dt) - timedelta(days=5)
        max_date = datetime.now()
        stock = yf.Ticker(self.ticker)
        stock_data = stock.history(start=min_date.strftime(
            '%Y-%m-%d'), end=max_date.strftime('%Y-%m-%d'))

        # Create a dictionary of stock prices with dates
        stock_prices = {}
        for date in date_list_dt:
            adjusted_date = date
            attempts = 0
            max_attempts = 10  # Increase attempts to handle long holiday periods
            while adjusted_date.strftime('%Y-%m-%d') not in stock_data.index.strftime('%Y-%m-%d') and attempts < max_attempts:
                adjusted_date -= timedelta(days=1)
                attempts += 1
            if adjusted_date.strftime('%Y-%m-%d') in stock_data.index.strftime('%Y-%m-%d'):
                stock_price = stock_data.loc[adjusted_date.strftime(
                    '%Y-%m-%d')]['Close']
                stock_prices[date.strftime('%Y-%m-%d')] = stock_price
            else:
                print(
                    f"No stock data found for date {date.strftime('%Y-%m-%d')}. Skipping.")

        pe_ratios = []
        for item in eps_filtered:
            end_date = item['end']
            eps = float(item['value'])
            fy = item.get('fy', 'N/A')
            fp = item.get('fp', 'N/A')
            stock_price = stock_prices.get(end_date)
            if stock_price and eps != 0:
                pe_ratio = stock_price / eps
                pe_data_point = {
                    'fy': fy,
                    'fp': fp,
                    'end_date': end_date,
                    'eps_diluted': eps,
                    'stock_price': stock_price,
                    'pe_ratio': pe_ratio
                }
                pe_ratios.append(pe_data_point)
            else:
                print(
                    f"Stock price or EPS not available for date {end_date}. Skipping.")

        # Sort PE ratios by end date
        pe_ratios.sort(key=lambda x: datetime.strptime(
            x['end_date'], '%Y-%m-%d'))
        return pe_ratios

    def calculate_pe_ratios(self):
        """
        Calculate PE ratios over time for Quarterly and Annual periods.

        Returns:
            dict: A dictionary containing PE ratios for Quarterly and Annual periods.
        """
        pe_ratios_quarterly = self.calculate_pe_ratios_over_time(
            period_type='Quarterly')
        pe_ratios_annual = self.calculate_pe_ratios_over_time(
            period_type='Annual')
        return {
            'pe_ratios_quarterly': pe_ratios_quarterly,
            'pe_ratios_annually': pe_ratios_annual
        }


# Example usage:
if __name__ == "__main__":
    # Initialize the SEC class with your User-Agent
    user_agent = 'YourAppName/1.0 (youremail@example.com)'
    sec = SEC(user_agent)

    # Initialize the FundamentalAnalysis with the SEC instance and the ticker symbol
    ticker_symbol = 'amzn'  # Change to your desired ticker symbol
    fa = FundamentalAnalysis(sec, ticker_symbol)

    # Fetch financial data starting from 2015
    fa.fetch_financial_data(start_date='2023-01-01')

    # Fetch stock data (max period to cover all dates)
    fa.fetch_stock_data()

    # Calculate PE TTM
    pe_ttm_data = fa.calculate_pe_ttm()
    print("PE TTM Data:")
    print(json.dumps(pe_ttm_data, indent=4))

    # Calculate PE Ratios Over Time
    pe_ratios_data = fa.calculate_pe_ratios()
    print("\nPE Ratios Over Time:")
    print(json.dumps(pe_ratios_data, indent=4))

    # Calculate Market Cap
    market_cap_data = fa.calculate_market_cap()
    print("\nMarket Cap Data:")
    print(json.dumps(market_cap_data, indent=4))
