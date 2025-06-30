import requests
import json
import logging
import time
import os
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, List, Optional, Union
import yfinance as yf  # Used for fetching stock prices and market data
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Configuration constants
SEC_API_BASE_URL = "https://data.sec.gov/api/xbrl"
SEC_TICKER_URL = "https://www.sec.gov/include/ticker.txt"
SEC_SEARCH_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
DEFAULT_RATE_LIMIT = 10  # requests per second as per SEC requirements
DEFAULT_USER_AGENT = "SEC-Financial-Pipeline/1.0"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def rate_limit(max_calls_per_second: int = DEFAULT_RATE_LIMIT):
    """Decorator to enforce rate limiting on API calls."""
    def decorator(func):
        last_called = [0.0]
        min_interval = 1.0 / max_calls_per_second
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                time.sleep(left_to_wait)
            result = func(*args, **kwargs)
            last_called[0] = time.time()
            return result
        return wrapper
    return decorator


def create_session_with_retries() -> requests.Session:
    """Create a requests session with automatic retries."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class SECDataCache:
    """Simple in-memory cache for SEC data."""
    
    def __init__(self, ttl: int = 3600):  # 1 hour default TTL
        self.cache = {}
        self.ttl = ttl
        
    def get(self, key: str) -> Optional[Dict]:
        """Get item from cache if not expired."""
        if key in self.cache:
            timestamp, data = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return data
            else:
                del self.cache[key]
        return None
        
    def set(self, key: str, data: Dict):
        """Set item in cache with timestamp."""
        self.cache[key] = (time.time(), data)
        
    def clear(self):
        """Clear all cached data."""
        self.cache.clear()


class SEC:
    def __init__(self, user_agent: str = None, rate_limit_per_second: int = DEFAULT_RATE_LIMIT, 
                 enable_cache: bool = True, cache_ttl: int = 3600):
        """
        Initialize the SEC class with enhanced production-ready features.

        Args:
            user_agent (str): The User-Agent string to be used in HTTP headers.
                            If None, uses environment variable SEC_USER_AGENT or default.
            rate_limit_per_second (int): Maximum API calls per second (default: 10).
            enable_cache (bool): Whether to enable response caching (default: True).
            cache_ttl (int): Cache time-to-live in seconds (default: 3600).
        """
        # Set user agent from parameter, environment variable, or default
        self.user_agent = (
            user_agent or 
            os.getenv('SEC_USER_AGENT', DEFAULT_USER_AGENT)
        )
        
        # Validate user agent format (SEC requires company name and email)
        if not self._validate_user_agent(self.user_agent):
            logger.warning(
                "User-Agent may not comply with SEC requirements. "
                "Please use format: 'CompanyName/Version (email@example.com)'"
            )
        
        self.headers = {'User-Agent': self.user_agent}
        self.rate_limit_per_second = rate_limit_per_second
        self.session = create_session_with_retries()
        
        # Initialize cache if enabled
        self.cache = SECDataCache(ttl=cache_ttl) if enable_cache else None
        
        logger.info(f"SEC client initialized with user agent: {self.user_agent}")
        logger.info(f"Rate limit: {rate_limit_per_second} requests/second")
        
        self.all_metrics = {
            # Core metrics
            'shares_outstanding': 'CommonStockSharesOutstanding',
            'eps_basic': 'EarningsPerShareBasic',
            'eps_diluted': 'EarningsPerShareDiluted',
            'dividends_per_share': 'CommonStockDividendsPerShareDeclared',
            'revenue': 'Revenues',
            'net_income': 'NetIncomeLoss',
            'total_assets': 'Assets',
            'total_liabilities': 'Liabilities',
            'equity': 'StockholdersEquity',
            'cash_and_cash_equivalents_end': 'CashAndCashEquivalentsAtCarryingValue',
            'cash_and_cash_equivalents_begin': 'CashAndCashEquivalentsAtCarryingValue',
            'net_change_in_cash': 'CashAndCashEquivalentsPeriodIncreaseDecrease',
            'operating_cash_flow': 'NetCashProvidedByUsedInOperatingActivities',
            'investing_cash_flow': 'NetCashProvidedByUsedInInvestingActivities',
            'financing_cash_flow': 'NetCashProvidedByUsedInFinancingActivities',
            'capital_expenditures': 'PaymentsToAcquirePropertyPlantAndEquipment',
            'gross_profit': 'GrossProfit',
            'operating_income': 'OperatingIncomeLoss',
            'research_and_development_expense': 'ResearchAndDevelopmentExpense',
            'short_term_investments': 'ShortTermInvestments',
            'marketable_securities_current': 'MarketableSecuritiesCurrent',
            # Additional possible metrics for Berkshire Hathaway
            'available_for_sale_securities_current': 'AvailableForSaleSecuritiesCurrent',
            'investments_in_debt_and_equity_securities': 'InvestmentsInDebtAndEquitySecurities',
            'investment_securities': 'InvestmentSecurities',
            'trading_account_assets': 'TradingAccountAssets',
            
            # Income Statement Metrics
            'cost_of_revenue': 'CostOfRevenue',
            'cost_of_goods_sold': 'CostOfGoodsAndServicesSold',
            'selling_general_admin_expense': 'SellingGeneralAndAdministrativeExpense',
            'interest_expense': 'InterestExpense',
            'interest_income': 'InterestIncomeOperating',
            'income_tax_expense': 'IncomeTaxExpenseBenefit',
            'depreciation_expense': 'DepreciationDepletionAndAmortization',
            'ebitda': 'EarningsBeforeInterestTaxesDepreciationAndAmortization',
            
            # Balance Sheet Metrics - Assets  
            'current_assets': 'AssetsCurrent',
            'noncurrent_assets': 'AssetsNoncurrent',
            'accounts_receivable': 'AccountsReceivableNetCurrent',
            'inventory': 'InventoryNet',
            'prepaid_expenses': 'PrepaidExpenseAndOtherAssets',
            'property_plant_equipment': 'PropertyPlantAndEquipmentNet',
            'goodwill': 'Goodwill',
            'intangible_assets': 'IntangibleAssetsNetExcludingGoodwill',
            'other_assets': 'OtherAssets',
            
            # Balance Sheet Metrics - Liabilities
            'current_liabilities': 'LiabilitiesCurrent',
            'noncurrent_liabilities': 'LiabilitiesNoncurrent',
            'accounts_payable': 'AccountsPayableCurrent',
            'accrued_liabilities': 'AccruedLiabilitiesCurrent',
            'short_term_debt': 'ShortTermBorrowings',
            'long_term_debt': 'LongTermDebt',
            'deferred_tax_liabilities': 'DeferredTaxLiabilitiesNoncurrent',
            
            # Balance Sheet Metrics - Equity
            'common_stock': 'CommonStockValue',
            'retained_earnings': 'RetainedEarningsAccumulatedDeficit',
            'additional_paid_in_capital': 'AdditionalPaidInCapitalCommonStock',
            'treasury_stock': 'TreasuryStockValue',
            'accumulated_other_comprehensive_income': 'AccumulatedOtherComprehensiveIncomeLoss',
            
            # Cash Flow Statement Metrics
            'depreciation_and_amortization': 'DepreciationDepletionAndAmortization',
            'stock_based_compensation': 'ShareBasedCompensation',
            'accounts_receivable_change': 'IncreaseDecreaseInAccountsReceivable',
            'inventory_change': 'IncreaseDecreaseInInventories',
            'accounts_payable_change': 'IncreaseDecreaseInAccountsPayable',
            'deferred_income_taxes': 'DeferredIncomeTaxExpenseBenefit',
            'acquisitions': 'PaymentsToAcquireBusinessesNetOfCashAcquired',
            'dispositions': 'ProceedsFromDivestitureOfBusinesses',
            'dividends_paid': 'PaymentsOfDividends',
            'stock_repurchases': 'PaymentsForRepurchaseOfCommonStock',
            'debt_issuance': 'ProceedsFromIssuanceOfLongTermDebt',
            'debt_repayment': 'RepaymentsOfLongTermDebt',
            
            # Per Share Metrics
            'book_value_per_share': 'BookValuePerShare',
            'tangible_book_value_per_share': 'TangibleBookValuePerShare',
            'cash_per_share': 'CashAndCashEquivalentsPerShare',
            
            # Additional Financial Ratios and Metrics
            'working_capital': 'WorkingCapital',
            'net_debt': 'NetDebt',
            'enterprise_value': 'EnterpriseValue',
            'market_capitalization': 'MarketCapitalization',
            
            # Segment and Geographic Data
            'revenue_from_external_customers': 'RevenueFromExternalCustomers',
            'intersegment_revenues': 'IntersegmentRevenues',
            
            # Industry-Specific Metrics
            'loan_loss_provision': 'ProvisionForLoanAndLeaseLosses',  # Banking
            'net_interest_income': 'InterestIncomeExpenseNet',  # Banking
            'premium_revenue': 'PremiumsEarnedNet',  # Insurance
            'investment_income': 'InvestmentIncomeNet',  # Insurance/Financial
            'oil_gas_reserves': 'ProvedOilAndGasReserves',  # Energy
            'same_store_sales': 'SameStoreSalesGrowth',  # Retail
            
            # Alternative metric names (some companies use different XBRL tags)
            'total_revenue': 'RevenueFromContractWithCustomerExcludingAssessedTax',
            'net_sales': 'SalesRevenueNet',
            'service_revenue': 'RevenueFromContractWithCustomerIncludingAssessedTax',
            'product_revenue': 'ProductSales',
            'subscription_revenue': 'SubscriptionRevenue',
        }

    def _validate_user_agent(self, user_agent: str) -> bool:
        """
        Validate that the user agent meets SEC requirements.
        
        Args:
            user_agent (str): The user agent string to validate.
            
        Returns:
            bool: True if user agent appears to meet SEC requirements.
        """
        # SEC recommends format: "CompanyName/Version (email@example.com)"
        # Check for presence of email pattern
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        return bool(re.search(email_pattern, user_agent))

    @rate_limit()
    def get_cik_from_ticker(self, ticker: str) -> Optional[str]:
        """
        Retrieve the Central Index Key (CIK) for a given ticker symbol.

        Args:
            ticker (str): The stock ticker symbol.

        Returns:
            str: The CIK number padded to 10 digits, or None if not found.
        """
        ticker = ticker.upper().strip()
        cache_key = f"cik_mapping_{ticker}"
        
        # Check cache first
        if self.cache:
            cached_result = self.cache.get(cache_key)
            if cached_result:
                logger.debug(f"Cache hit for ticker {ticker}")
                return cached_result.get('cik')
        
        try:
            logger.info(f"Fetching CIK for ticker: {ticker}")
            response = self.session.get(SEC_TICKER_URL, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            content = response.text
            lines = content.strip().split('\n')
            ticker_to_cik = {}
            
            for line in lines:
                parts = line.split()
                if len(parts) != 2:
                    continue
                ticker_line, cik_line = parts
                ticker_to_cik[ticker_line.lower()] = cik_line
            
            cik = ticker_to_cik.get(ticker.lower())
            if cik:
                cik = cik.zfill(10)
                logger.info(f"Found CIK {cik} for ticker {ticker}")
                
                # Cache the result
                if self.cache:
                    self.cache.set(cache_key, {'cik': cik})
                    
                return cik
            else:
                logger.warning(f"CIK not found for ticker {ticker}. Attempting manual search.")
                cik = self.search_cik_manual(ticker)
                
                # Cache the result even if None to avoid repeated searches
                if self.cache:
                    self.cache.set(cache_key, {'cik': cik})
                    
                return cik
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve ticker to CIK mapping: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_cik_from_ticker: {e}")
            return None

    @rate_limit()
    def search_cik_manual(self, ticker: str) -> Optional[str]:
        """
        Manually search for CIK using company name.

        Args:
            ticker (str): The stock ticker symbol.

        Returns:
            str: The CIK number if found, else None.
        """
        try:
            search_url = f"{SEC_SEARCH_URL}?CIK={ticker}&owner=exclude&action=getcompany&Find=Search"
            logger.info(f"Manual CIK search for ticker: {ticker}")
            
            response = self.session.get(search_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # Parse the response to extract CIK
            content = response.text
            match = re.search(r'CIK=(\d{10})', content)
            if match:
                cik = match.group(1)
                logger.info(f"Manual search found CIK {cik} for ticker {ticker}")
                return cik
            else:
                logger.warning(f"Manual search: CIK not found for ticker {ticker}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error in manual CIK search for ticker {ticker}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in manual CIK search: {e}")
            return None

    @rate_limit()
    def get_company_facts(self, cik: str) -> Dict:
        """
        Fetch company facts data from the SEC's EDGAR API with caching and error handling.

        Args:
            cik (str): The Central Index Key of the company.

        Returns:
            dict: The JSON data retrieved from the SEC's API.
        """
        if not cik:
            logger.error("CIK is required but not provided")
            return {}
            
        # Ensure CIK is properly formatted
        cik = str(cik).zfill(10)
        cache_key = f"company_facts_{cik}"
        
        # Check cache first
        if self.cache:
            cached_result = self.cache.get(cache_key)
            if cached_result:
                logger.debug(f"Cache hit for company facts CIK {cik}")
                return cached_result
        
        try:
            url = f'{SEC_API_BASE_URL}/companyfacts/CIK{cik}.json'
            logger.info(f"Fetching company facts for CIK: {cik}")
            
            response = self.session.get(url, headers=self.headers, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            # Validate the response structure
            if not isinstance(data, dict):
                logger.error(f"Invalid response format for CIK {cik}")
                return {}
                
            if 'facts' not in data:
                logger.warning(f"No facts section found in response for CIK {cik}")
                return {}
            
            logger.info(f"Successfully retrieved company facts for CIK {cik}")
            
            # Cache the result
            if self.cache:
                self.cache.set(cache_key, data)
                
            return data
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.error(f"Company facts not found for CIK {cik} (404)")
            else:
                logger.error(f"HTTP error retrieving company facts for CIK {cik}: {e}")
            return {}
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error retrieving company facts for CIK {cik}: {e}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for CIK {cik}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error retrieving company facts for CIK {cik}: {e}")
            return {}

    def get_general_info(self, data, user_ticker):
        """
        Extract general information from the company facts data.

        Args:
            data (dict): The JSON data retrieved from the SEC's API.
            user_ticker (str): The stock ticker symbol.

        Returns:
            dict: A dictionary containing general company information.
        """
        general_info = {
            'ticker': user_ticker.upper(),
            'company_name': data.get('entityName', 'N/A'),
            'cik': str(data.get('cik', 'N/A')).zfill(10)
        }
        return general_info

    def extract_metric(self, facts, metric_name, start_date, end_date):
        """
        Extract a specific financial metric from the facts data, searching all namespaces.

        Args:
            facts (dict): The 'facts' section from the company facts JSON.
            metric_name (str): The name of the metric to extract.
            start_date (datetime, optional): The start date for filtering data.
            end_date (datetime, optional): The end date for filtering data.

        Returns:
            list: A list of dictionaries containing the extracted data points.
        """
        data_list = []
        # Search through all namespaces
        for namespace, metrics in facts.items():
            metric = metrics.get(metric_name, {})
            if not metric:
                continue
            units = metric.get('units', {})
            for unit in units:
                for item in units[unit]:
                    end_date_str = item.get('end', 'N/A')
                    start_date_str = item.get('start', 'N/A')
                    if end_date_str == 'N/A':
                        continue
                    try:
                        item_end_date = datetime.strptime(
                            end_date_str, '%Y-%m-%d')
                    except ValueError:
                        continue
                    if start_date and item_end_date < start_date:
                        continue
                    if end_date and item_end_date > end_date:
                        continue
                    # Determine period length
                    if 'start' in item and item['start'] != 'N/A':
                        try:
                            item_start_date = datetime.strptime(
                                item['start'], '%Y-%m-%d')
                        except ValueError:
                            item_start_date = None
                    else:
                        item_start_date = None
                    if item_start_date:
                        period_days = (item_end_date - item_start_date).days
                        if period_days <= 95:
                            period_type = 'Quarterly'
                        elif period_days <= 185:
                            period_type = 'Half-Year'
                        elif period_days <= 280:
                            period_type = 'Year-to-Date'
                        else:
                            period_type = 'Annual'
                    else:
                        period_type = 'Instant'

                    # Convert value to float if possible
                    try:
                        value = float(item.get('val', 'N/A'))
                    except (ValueError, TypeError):
                        value = None

                    data_point = {
                        'start': item.get('start', 'N/A'),
                        'end': item.get('end', 'N/A'),
                        'value': value,
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
            start_date (str, optional): The start date in 'YYYY-MM-DD' format.
            end_date (str, optional): The end date in 'YYYY-MM-DD' format.
            metrics_to_retrieve (list, optional): List of financial data keys to retrieve.
                                                  If None, all available data is retrieved.

        Returns:
            dict: A dictionary containing the financial data and general information.
        """
        date_format = "%Y-%m-%d"
        if start_date:
            try:
                start_date = datetime.strptime(start_date, date_format)
            except ValueError:
                print(
                    f"Invalid start_date format: {start_date}. Expected 'YYYY-MM-DD'.")
                start_date = None
        if end_date:
            try:
                end_date = datetime.strptime(end_date, date_format)
            except ValueError:
                print(
                    f"Invalid end_date format: {end_date}. Expected 'YYYY-MM-DD'.")
                end_date = None

        cik = self.get_cik_from_ticker(ticker)
        if not cik:
            print(f"CIK not found for ticker {ticker}")
            return {}
        data = self.get_company_facts(cik)
        facts = data.get('facts', {})

        general_info = self.get_general_info(data, ticker)

        # If metrics_to_retrieve is None, retrieve all metrics
        if metrics_to_retrieve is None:
            metrics_to_retrieve = list(self.all_metrics.keys())

        financial_data = {
            'general_info': general_info
        }

        # Extract only the requested metrics
        for data_key in metrics_to_retrieve:
            metric_name = self.all_metrics.get(data_key)
            if metric_name:
                metric_data = self.extract_metric(
                    facts, metric_name, start_date, end_date)
                financial_data[data_key] = metric_data
            else:
                print(f"Metric '{data_key}' is not recognized.")

        return financial_data

    def list_available_metrics(self, ticker):
        """
        List all available financial metrics for a given ticker.

        Args:
            ticker (str): The stock ticker symbol.

        Returns:
            list: A list of available metric names.
        """
        cik = self.get_cik_from_ticker(ticker)
        if not cik:
            print(f"CIK not found for ticker {ticker}")
            return []
        data = self.get_company_facts(cik)
        facts = data.get('facts', {})
        available_metrics = []
        for namespace, metrics in facts.items():
            for metric_name in metrics.keys():
                full_metric_name = f"{namespace}:{metric_name}"
                available_metrics.append(full_metric_name)
        return available_metrics


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
            start_date (str, optional): The start date in 'YYYY-MM-DD' format.
            end_date (str, optional): The end date in 'YYYY-MM-DD' format.
            metrics_to_retrieve (list, optional): List of financial data keys to retrieve.
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
            start_date (str, optional): The start date in 'YYYY-MM-DD' format.
            end_date (str, optional): The end date in 'YYYY-MM-DD' format.
        """
        # Adjust ticker symbol for yfinance
        yf_ticker = self.ticker.replace('.', '-')
        stock = yf.Ticker(yf_ticker)
        try:
            if start_date and end_date:
                self.stock_data = stock.history(start=start_date, end=end_date)
            else:
                self.stock_data = stock.history(period='max')
        except Exception as e:
            print(f"Error fetching stock data: {e}")
            self.stock_data = None

    def get_latest_stock_price(self):
        """
        Get the latest stock price.

        Returns:
            float: The latest closing stock price.
        """
        if self.stock_data is None:
            self.fetch_stock_data()
        if not self.stock_data.empty:
            latest_price = self.stock_data['Close'].iloc[-1]
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
        try:
            eps_ttm = sum(float(item['value']) for item in last_four_quarters)
        except (ValueError, TypeError):
            print("Invalid EPS values encountered.")
            return {}

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
        yf_ticker = self.ticker.replace('.', '-')
        stock = yf.Ticker(yf_ticker)
        try:
            stock_data = stock.history(start=min_date.strftime(
                '%Y-%m-%d'), end=max_date.strftime('%Y-%m-%d'))
        except Exception as e:
            print(f"Error fetching stock data: {e}")
            return []

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

    def calculate_ending_cash_balance(self):
        """
        Calculate the ending cash balance using cash flows and beginning cash balance.

        Returns:
            dict: A dictionary containing the ending cash balance and related information.
        """
        if not self.financial_data:
            self.fetch_financial_data()

        # Get cash flows
        operating_cf = self.financial_data.get('operating_cash_flow', [])
        investing_cf = self.financial_data.get('investing_cash_flow', [])
        financing_cf = self.financial_data.get('financing_cash_flow', [])

        # Get beginning and ending cash balances
        beginning_cash = self.financial_data.get(
            'cash_and_cash_equivalents_begin', [])
        ending_cash = self.financial_data.get(
            'cash_and_cash_equivalents_end', [])

        # Ensure data is available
        if not (operating_cf and investing_cf and financing_cf and beginning_cash):
            print("Not all necessary data is available to calculate ending cash balance.")
            return {}

        # Get the latest periods
        operating_cf_latest = sorted(
            operating_cf, key=lambda x: datetime.strptime(x['end'], '%Y-%m-%d'))[-1]
        investing_cf_latest = sorted(
            investing_cf, key=lambda x: datetime.strptime(x['end'], '%Y-%m-%d'))[-1]
        financing_cf_latest = sorted(
            financing_cf, key=lambda x: datetime.strptime(x['end'], '%Y-%m-%d'))[-1]
        beginning_cash_latest = sorted(
            beginning_cash, key=lambda x: datetime.strptime(x['end'], '%Y-%m-%d'))[-1]

        # Calculate net change in cash
        try:
            net_change_in_cash = float(operating_cf_latest['value']) + float(
                investing_cf_latest['value']) + float(financing_cf_latest['value'])
        except (ValueError, TypeError):
            print("Invalid cash flow values encountered.")
            net_change_in_cash = None

        if net_change_in_cash is None:
            print("Cannot calculate net change in cash due to invalid data.")
            return {}

        # Get beginning cash balance
        try:
            beginning_cash_balance = float(beginning_cash_latest['value'])
        except (ValueError, TypeError):
            print("Invalid beginning cash balance value.")
            beginning_cash_balance = None

        if beginning_cash_balance is None:
            print(
                "Cannot calculate ending cash balance due to invalid beginning balance.")
            return {}

        # Calculate ending cash balance
        calculated_ending_cash_balance = beginning_cash_balance + net_change_in_cash

        # Compare with reported ending cash balance if available
        if ending_cash:
            try:
                ending_cash_latest = sorted(
                    ending_cash, key=lambda x: datetime.strptime(x['end'], '%Y-%m-%d'))[-1]
                reported_ending_cash_balance = float(
                    ending_cash_latest['value'])
                difference = reported_ending_cash_balance - calculated_ending_cash_balance
            except (ValueError, TypeError):
                reported_ending_cash_balance = None
                difference = None
        else:
            reported_ending_cash_balance = None
            difference = None

        return {
            'beginning_cash_balance': beginning_cash_balance,
            'operating_cash_flow': float(operating_cf_latest['value']),
            'investing_cash_flow': float(investing_cf_latest['value']),
            'financing_cash_flow': float(financing_cf_latest['value']),
            'net_change_in_cash': net_change_in_cash,
            'calculated_ending_cash_balance': calculated_ending_cash_balance,
            'reported_ending_cash_balance': reported_ending_cash_balance,
            'difference': difference
        }

    def calculate_free_cash_flow(self):
        """
        Calculate Free Cash Flow (FCF) using operating cash flow and capital expenditures.

        Returns:
            list: A list of dictionaries containing FCF data over time.
        """
        if not self.financial_data:
            self.fetch_financial_data()

        # Get operating cash flow data
        operating_cf = self.financial_data.get('operating_cash_flow', [])
        if not operating_cf:
            print("Operating cash flow data not available.")
            return []

        # Get capital expenditures data
        capex_data = self.financial_data.get('capital_expenditures', [])
        if not capex_data:
            print("Capital expenditures data not available.")
            return []

        # Sort data by end date
        operating_cf.sort(
            key=lambda x: datetime.strptime(x['end'], '%Y-%m-%d'))
        capex_data.sort(key=lambda x: datetime.strptime(x['end'], '%Y-%m-%d'))

        # Align the periods for OCF and CAPEX
        fcf_data = []
        for ocf_item in operating_cf:
            ocf_end_date = ocf_item['end']
            try:
                ocf_value = float(ocf_item['value'])
            except (ValueError, TypeError):
                print(
                    f"Invalid operating cash flow value for end date {ocf_end_date}. Skipping.")
                continue

            # Find matching CAPEX item
            capex_item = next(
                (item for item in capex_data if item['end'] == ocf_end_date), None)
            if capex_item:
                try:
                    capex_value = float(capex_item['value'])
                except (ValueError, TypeError):
                    print(
                        f"Invalid capital expenditures value for end date {ocf_end_date}. Skipping.")
                    continue
                fcf_value = ocf_value - capex_value
                fcf_data_point = {
                    'fy': ocf_item.get('fy', 'N/A'),
                    'fp': ocf_item.get('fp', 'N/A'),
                    'end_date': ocf_end_date,
                    'operating_cash_flow': ocf_value,
                    'capital_expenditures': capex_value,
                    'free_cash_flow': fcf_value,
                    'period_type': ocf_item.get('period_type', 'N/A')
                }
                fcf_data.append(fcf_data_point)
            else:
                print(
                    f"CAPEX data not available for end date {ocf_end_date}. Skipping.")

        return fcf_data

    def calculate_cash_and_short_term_investments(self):
        """
        Calculate Cash and Short-Term Investments over time.

        Returns:
            list: A list of dictionaries containing the combined cash and short-term investments over time.
        """
        if not self.financial_data:
            self.fetch_financial_data()

        # Initialize an empty dictionary to hold all metrics by end date
        data_by_end_date = {}

        # Aggregate cash and equivalents
        cash_metrics = [
            'cash_and_cash_equivalents',
            'cash_and_cash_equivalents_end',
            # Add any other cash-related metrics if needed
        ]

        for metric_key in cash_metrics:
            metric_data = self.financial_data.get(metric_key, [])
            for item in metric_data:
                end_date = item['end']
                value = item['value']
                if value is None:
                    continue
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    print(
                        f"Invalid value for metric '{metric_key}' on date {end_date}. Skipping.")
                    continue
                data_point = data_by_end_date.setdefault(end_date, {
                    'fy': item.get('fy', 'N/A'),
                    'fp': item.get('fp', 'N/A'),
                    'end_date': end_date,
                    'cash_and_cash_equivalents': 0.0,
                    'short_term_investments': 0.0,
                    'cash_and_short_term_investments': 0.0,
                    'period_type': item.get('period_type', 'N/A')
                })
                data_point['cash_and_cash_equivalents'] += value
                data_point['cash_and_short_term_investments'] += value

        # Aggregate short-term investments
        sti_metrics = [
            'short_term_investments',
            'marketable_securities_current',
            'available_for_sale_securities_current',
            'investments_in_debt_and_equity_securities',
            'investment_securities',
            'trading_account_assets',
            # Add any other investment-related metrics if needed
        ]

        for metric_key in sti_metrics:
            metric_data = self.financial_data.get(metric_key, [])
            for item in metric_data:
                end_date = item['end']
                value = item['value']
                if value is None:
                    continue
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    print(
                        f"Invalid value for metric '{metric_key}' on date {end_date}. Skipping.")
                    continue
                data_point = data_by_end_date.setdefault(end_date, {
                    'fy': item.get('fy', 'N/A'),
                    'fp': item.get('fp', 'N/A'),
                    'end_date': end_date,
                    'cash_and_cash_equivalents': 0.0,
                    'short_term_investments': 0.0,
                    'cash_and_short_term_investments': 0.0,
                    'period_type': item.get('period_type', 'N/A')
                })
                data_point['short_term_investments'] += value
                data_point['cash_and_short_term_investments'] += value

        # Convert the dictionary to a sorted list
        combined_data = list(data_by_end_date.values())
        combined_data.sort(key=lambda x: datetime.strptime(
            x['end_date'], '%Y-%m-%d'))

        return combined_data

    def filter_financial_data(self, selected_metrics):
        """
        Filter the financial data to include only the selected metrics.

        Args:
            selected_metrics (list): List of metric keys to include.

        Returns:
            dict: Filtered financial data containing only the selected metrics.
        """
        if not self.financial_data:
            print("Financial data not fetched yet.")
            return {}

        filtered_data = {
            'general_info': self.financial_data.get('general_info', {})
        }

        for metric_key in selected_metrics:
            metric_data = self.financial_data.get(metric_key)
            if metric_data:
                filtered_data[metric_key] = metric_data
            else:
                print(f"Metric '{metric_key}' not found in financial data.")

        return filtered_data


if __name__ == "__main__":
    # Initialize the SEC class with your User-Agent
    # Replace with your actual User-Agent
    user_agent = 'YourAppName/1.0 (youremail@example.com)'
    sec = SEC(user_agent)

    # Initialize the FundamentalAnalysis with the SEC instance and the ticker symbol
    ticker_symbol = 'TSLA'  # Change to your desired ticker symbol, e.g., 'TSLA'
    fa = FundamentalAnalysis(sec, ticker_symbol)

    # Example Usage:

    # 1. List all available metrics for the ticker
    print(f"Listing all available metrics for {ticker_symbol}...")
    available_metrics = sec.list_available_metrics(ticker_symbol)
    print("\nAvailable Metrics:")
    for metric in available_metrics:
        print(metric)

    # 2. Define metrics to retrieve (allowing user to select one or multiple)
    # For example, using 'shares_outstanding' as the selected metric
    selected_metrics = ['shares_outstanding']

    # 3. Fetch financial data with selected metrics and date range
    start_date = '2009-01-01'  # Change as needed
    end_date = '2023-12-31'    # Change as needed
    fa.fetch_financial_data(start_date=start_date,
                            end_date=end_date,
                            metrics_to_retrieve=selected_metrics)

    # 4. Optionally, filter the financial data (if additional filtering is needed)
    # For this example, since we've already selected metrics during fetching, this step is redundant
    # But it's available for further filtering
    # filtered_financial_data = fa.filter_financial_data(selected_metrics)

    # 5. Fetch stock data (optional, based on your needs)
    fa.fetch_stock_data(start_date=start_date, end_date=end_date)

    # 6. Calculate and print Shares Outstanding Data
    shares_outstanding_data = fa.financial_data.get('shares_outstanding', [])
    print("\nShares Outstanding Data:")
    print(json.dumps(shares_outstanding_data, indent=4))

#     # 7. Calculate and print other financial metrics as needed
#     # Calculate Free Cash Flow
#     free_cash_flow_data = fa.calculate_free_cash_flow()
#     print("\nFree Cash Flow Data:")
#     print(json.dumps(free_cash_flow_data, indent=4))

#     # Calculate PE TTM
#     pe_ttm_data = fa.calculate_pe_ttm()
#     print("\nPE TTM Data:")
#     print(json.dumps(pe_ttm_data, indent=4))

#     # Calculate PE Ratios Over Time
#     pe_ratios_data = fa.calculate_pe_ratios()
#     print("\nPE Ratios Over Time:")
#     print(json.dumps(pe_ratios_data, indent=4))

#     # Calculate Ending Cash Balance
#     ending_cash_balance_data = fa.calculate_ending_cash_balance()
#     print("\nEnding Cash Balance Data:")
#     print(json.dumps(ending_cash_balance_data, indent=4))

#     # Calculate Market Cap
#     market_cap_data = fa.calculate_market_cap()
#     print("\nMarket Cap Data:")
#     print(json.dumps(market_cap_data, indent=4))
