import unittest
import json
import time
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import sys
import tempfile
import shutil

# Add the parent directory to sys.path to import the SEC module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'source'))

from sec import SEC, FundamentalAnalysis, SECDataCache, rate_limit, create_session_with_retries

class TestSECDataCache(unittest.TestCase):
    """Test the SECDataCache class."""
    
    def setUp(self):
        self.cache = SECDataCache(ttl=1)  # 1 second TTL for testing
    
    def test_cache_set_and_get(self):
        """Test setting and getting cache data."""
        test_data = {'test': 'data'}
        self.cache.set('test_key', test_data)
        
        result = self.cache.get('test_key')
        self.assertEqual(result, test_data)
    
    def test_cache_expiry(self):
        """Test that cache expires after TTL."""
        test_data = {'test': 'data'}
        self.cache.set('test_key', test_data)
        
        # Should get data immediately
        result = self.cache.get('test_key')
        self.assertEqual(result, test_data)
        
        # Wait for expiry
        time.sleep(1.1)
        
        # Should return None after expiry
        result = self.cache.get('test_key')
        self.assertIsNone(result)
    
    def test_cache_clear(self):
        """Test clearing cache."""
        self.cache.set('test_key', {'test': 'data'})
        self.cache.clear()
        
        result = self.cache.get('test_key')
        self.assertIsNone(result)


class TestRateLimit(unittest.TestCase):
    """Test the rate limiting decorator."""
    
    def test_rate_limit_decorator(self):
        """Test that rate limiting works."""
        call_times = []
        
        @rate_limit(max_calls_per_second=5)  # 5 calls per second
        def test_function():
            call_times.append(time.time())
            return "called"
        
        # Make multiple calls
        for _ in range(3):
            test_function()
        
        # Check that calls were spaced appropriately
        if len(call_times) >= 2:
            time_diff = call_times[1] - call_times[0]
            # Should be at least 0.2 seconds (1/5) between calls
            self.assertGreaterEqual(time_diff, 0.15)  # Allow some tolerance


class TestSECClass(unittest.TestCase):
    """Test the SEC class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.user_agent = "TestCompany/1.0 (test@example.com)"
        self.sec = SEC(user_agent=self.user_agent, enable_cache=True)
    
    def tearDown(self):
        """Clean up after tests."""
        if hasattr(self.sec, 'cache') and self.sec.cache:
            self.sec.cache.clear()
    
    def test_initialization(self):
        """Test SEC class initialization."""
        self.assertIsNotNone(self.sec)
        self.assertEqual(self.sec.user_agent, self.user_agent)
        self.assertIsNotNone(self.sec.headers)
        self.assertIsNotNone(self.sec.session)
        self.assertIsNotNone(self.sec.cache)
    
    def test_initialization_with_defaults(self):
        """Test SEC class initialization with default values."""
        sec = SEC()
        self.assertIsNotNone(sec.user_agent)
        self.assertIsNotNone(sec.headers)
        self.assertIsNotNone(sec.session)
    
    def test_user_agent_validation(self):
        """Test user agent validation."""
        # Valid user agent with email
        valid_agent = "TestCompany/1.0 (test@example.com)"
        self.assertTrue(self.sec._validate_user_agent(valid_agent))
        
        # Invalid user agent without email
        invalid_agent = "TestCompany/1.0"
        self.assertFalse(self.sec._validate_user_agent(invalid_agent))
    
    @patch('sec.requests.Session.get')
    def test_get_cik_from_ticker_success(self, mock_get):
        """Test successful CIK retrieval from ticker."""
        # Mock the ticker.txt response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "aapl\t0000320193\ntsla\t0001318605\n"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        cik = self.sec.get_cik_from_ticker("AAPL")
        self.assertEqual(cik, "0000320193")
    
    @patch('sec.requests.Session.get')
    def test_get_cik_from_ticker_not_found(self, mock_get):
        """Test CIK retrieval when ticker not found."""
        # Mock the ticker.txt response without the requested ticker
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "aapl\t0000320193\ntsla\t0001318605\n"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        with patch.object(self.sec, 'search_cik_manual', return_value=None):
            cik = self.sec.get_cik_from_ticker("NOTFOUND")
            self.assertIsNone(cik)
    
    @patch('sec.requests.Session.get')
    def test_get_cik_from_ticker_with_cache(self, mock_get):
        """Test CIK retrieval with caching."""
        # Mock the ticker.txt response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "aapl\t0000320193\n"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # First call should make HTTP request
        cik1 = self.sec.get_cik_from_ticker("AAPL")
        self.assertEqual(cik1, "0000320193")
        self.assertEqual(mock_get.call_count, 1)
        
        # Second call should use cache
        cik2 = self.sec.get_cik_from_ticker("AAPL")
        self.assertEqual(cik2, "0000320193")
        self.assertEqual(mock_get.call_count, 1)  # Should not increase
    
    @patch('sec.requests.Session.get')
    def test_search_cik_manual_success(self, mock_get):
        """Test manual CIK search success."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '<a href="/cgi-bin/browse-edgar?CIK=0000320193&owner=exclude">'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        cik = self.sec.search_cik_manual("AAPL")
        self.assertEqual(cik, "0000320193")
    
    @patch('sec.requests.Session.get')
    def test_search_cik_manual_not_found(self, mock_get):
        """Test manual CIK search when not found."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '<html>No results found</html>'
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        cik = self.sec.search_cik_manual("NOTFOUND")
        self.assertIsNone(cik)
    
    @patch('sec.requests.Session.get')
    def test_get_company_facts_success(self, mock_get):
        """Test successful company facts retrieval."""
        mock_data = {
            "cik": 320193,
            "entityName": "Apple Inc.",
            "facts": {
                "us-gaap": {
                    "Revenue": {
                        "units": {
                            "USD": [
                                {"val": 365817000000, "end": "2021-09-25", "fy": 2021, "fp": "FY"}
                            ]
                        }
                    }
                }
            }
        }
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        data = self.sec.get_company_facts("0000320193")
        self.assertEqual(data["entityName"], "Apple Inc.")
        self.assertIn("facts", data)
    
    @patch('sec.requests.Session.get')
    def test_get_company_facts_with_cache(self, mock_get):
        """Test company facts retrieval with caching."""
        mock_data = {"cik": 320193, "entityName": "Apple Inc.", "facts": {}}
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # First call
        data1 = self.sec.get_company_facts("0000320193")
        self.assertEqual(mock_get.call_count, 1)
        
        # Second call should use cache
        data2 = self.sec.get_company_facts("0000320193")
        self.assertEqual(mock_get.call_count, 1)  # Should not increase
        self.assertEqual(data1, data2)
    
    def test_get_general_info(self):
        """Test general info extraction."""
        mock_data = {
            "cik": 320193,
            "entityName": "Apple Inc."
        }
        
        info = self.sec.get_general_info(mock_data, "AAPL")
        self.assertEqual(info["ticker"], "AAPL")
        self.assertEqual(info["company_name"], "Apple Inc.")
        self.assertEqual(info["cik"], "0000320193")
    
    def test_extract_metric(self):
        """Test metric extraction from facts data."""
        facts_data = {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {
                                "val": 365817000000,
                                "end": "2021-09-25",
                                "start": "2020-09-26",
                                "fy": 2021,
                                "fp": "FY",
                                "form": "10-K",
                                "filed": "2021-10-29"
                            }
                        ]
                    }
                }
            }
        }
        
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2022, 1, 1)
        
        result = self.sec.extract_metric(facts_data, "Revenues", start_date, end_date)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["value"], 365817000000)
        self.assertEqual(result[0]["end"], "2021-09-25")
        self.assertEqual(result[0]["period_type"], "Annual")
    
    @patch.object(SEC, 'get_cik_from_ticker')
    @patch.object(SEC, 'get_company_facts')
    def test_get_financial_data_success(self, mock_get_facts, mock_get_cik):
        """Test successful financial data retrieval."""
        mock_get_cik.return_value = "0000320193"
        mock_get_facts.return_value = {
            "cik": 320193,
            "entityName": "Apple Inc.",
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "units": {
                            "USD": [
                                {"val": 365817000000, "end": "2021-09-25", "fy": 2021}
                            ]
                        }
                    }
                }
            }
        }
        
        data = self.sec.get_financial_data("AAPL", metrics_to_retrieve=["revenue"])
        
        self.assertIn("general_info", data)
        self.assertIn("revenue", data)
        self.assertEqual(data["general_info"]["ticker"], "AAPL")
    
    def test_list_available_metrics(self):
        """Test listing available metrics."""
        # Test that the method returns a list of available metrics keys
        metrics = list(self.sec.all_metrics.keys())
        self.assertIsInstance(metrics, list)
        self.assertIn("revenue", metrics)
        self.assertIn("net_income", metrics)
        self.assertIn("total_assets", metrics)


class TestFundamentalAnalysis(unittest.TestCase):
    """Test the FundamentalAnalysis class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.user_agent = "TestCompany/1.0 (test@example.com)"
        self.sec = SEC(user_agent=self.user_agent)
        self.fa = FundamentalAnalysis(self.sec, "AAPL")
    
    def test_initialization(self):
        """Test FundamentalAnalysis initialization."""
        self.assertIsNotNone(self.fa)
        self.assertEqual(self.fa.ticker, "AAPL")
        self.assertIsNone(self.fa.financial_data)
        self.assertIsNone(self.fa.stock_data)
    
    @patch.object(SEC, 'get_financial_data')
    def test_fetch_financial_data(self, mock_get_data):
        """Test fetching financial data."""
        mock_data = {
            "general_info": {"ticker": "AAPL", "company_name": "Apple Inc."},
            "revenue": [{"val": 365817000000, "end": "2021-09-25"}]
        }
        mock_get_data.return_value = mock_data
        
        self.fa.fetch_financial_data()
        
        self.assertIsNotNone(self.fa.financial_data)
        self.assertEqual(self.fa.financial_data["general_info"]["ticker"], "AAPL")
    
    @patch('yfinance.Ticker')
    def test_fetch_stock_data(self, mock_ticker):
        """Test fetching stock data."""
        mock_stock = Mock()
        mock_history = Mock()
        mock_history.empty = False
        mock_stock.history.return_value = mock_history
        mock_ticker.return_value = mock_stock
        
        self.fa.fetch_stock_data()
        
        self.assertIsNotNone(self.fa.stock_data)
    
    @patch('yfinance.Ticker')
    def test_get_latest_stock_price(self, mock_ticker):
        """Test getting latest stock price."""
        import pandas as pd
        
        # Create mock stock data
        mock_stock = Mock()
        mock_data = pd.DataFrame({
            'Close': [150.0, 155.0, 160.0]
        })
        mock_stock.history.return_value = mock_data
        mock_ticker.return_value = mock_stock
        
        # Set the stock_data directly to bypass the fetch
        self.fa.stock_data = mock_data
        
        price = self.fa.get_latest_stock_price()
        self.assertEqual(price, 160.0)
    
    def test_calculate_market_cap(self):
        """Test market cap calculation."""
        # Mock financial data
        self.fa.financial_data = {
            "shares_outstanding": [
                {"value": 16000000000, "end": "2021-09-25", "unit": "shares"}
            ]
        }
        
        with patch.object(self.fa, 'get_latest_stock_price', return_value=150.0):
            with patch('yfinance.Ticker') as mock_ticker:
                mock_stock = Mock()
                mock_stock.info = {'sharesOutstanding': 16000000000}
                mock_ticker.return_value = mock_stock
                
                result = self.fa.calculate_market_cap()
                
                expected_market_cap = 150.0 * 16000000000
                self.assertEqual(result['market_cap'], expected_market_cap)
                self.assertEqual(result['latest_stock_price'], 150.0)
                self.assertEqual(result['shares_outstanding'], 16000000000)
    
    def test_calculate_pe_ttm(self):
        """Test PE TTM calculation."""
        # Mock financial data with quarterly EPS
        self.fa.financial_data = {
            "eps_diluted": [
                {"value": 1.0, "end": "2021-06-30", "period_type": "Quarterly"},
                {"value": 1.1, "end": "2021-09-30", "period_type": "Quarterly"},
                {"value": 1.2, "end": "2021-12-31", "period_type": "Quarterly"},
                {"value": 1.3, "end": "2022-03-31", "period_type": "Quarterly"},
            ]
        }
        
        with patch.object(self.fa, 'get_latest_stock_price', return_value=150.0):
            result = self.fa.calculate_pe_ttm()
            
            expected_eps_ttm = 1.0 + 1.1 + 1.2 + 1.3  # 4.6
            expected_pe_ttm = 150.0 / 4.6
            
            self.assertAlmostEqual(result['pe_ttm'], expected_pe_ttm, places=2)
            self.assertEqual(result['eps_ttm'], expected_eps_ttm)
            self.assertEqual(result['latest_stock_price'], 150.0)
    
    def test_calculate_free_cash_flow(self):
        """Test free cash flow calculation."""
        self.fa.financial_data = {
            "operating_cash_flow": [
                {"value": 100000000, "end": "2021-09-25", "fy": 2021}
            ],
            "capital_expenditures": [
                {"value": 20000000, "end": "2021-09-25", "fy": 2021}
            ]
        }
        
        result = self.fa.calculate_free_cash_flow()
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["free_cash_flow"], 80000000)  # 100M - 20M
        self.assertEqual(result[0]["operating_cash_flow"], 100000000)
        self.assertEqual(result[0]["capital_expenditures"], 20000000)
    
    def test_calculate_cash_and_short_term_investments(self):
        """Test cash and short-term investments calculation."""
        self.fa.financial_data = {
            "cash_and_cash_equivalents_end": [
                {"value": 50000000, "end": "2021-09-25", "fy": 2021}
            ],
            "short_term_investments": [
                {"value": 30000000, "end": "2021-09-25", "fy": 2021}
            ]
        }
        
        result = self.fa.calculate_cash_and_short_term_investments()
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["cash_and_short_term_investments"], 80000000)
        self.assertEqual(result[0]["cash_and_cash_equivalents"], 50000000)
        self.assertEqual(result[0]["short_term_investments"], 30000000)
    
    def test_filter_financial_data(self):
        """Test filtering financial data."""
        self.fa.financial_data = {
            "general_info": {"ticker": "AAPL"},
            "revenue": [{"value": 1000000}],
            "net_income": [{"value": 100000}],
            "total_assets": [{"value": 5000000}]
        }
        
        filtered = self.fa.filter_financial_data(["revenue", "net_income"])
        
        self.assertIn("general_info", filtered)
        self.assertIn("revenue", filtered)
        self.assertIn("net_income", filtered)
        self.assertNotIn("total_assets", filtered)


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete SEC data pipeline."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.user_agent = "TestCompany/1.0 (test@example.com)"
    
    @unittest.skip("Integration test - requires internet connection")
    def test_full_pipeline_apple(self):
        """
        Full integration test with Apple (AAPL).
        This test is skipped by default as it requires internet connection.
        Remove @unittest.skip decorator to run this test.
        """
        sec = SEC(user_agent=self.user_agent)
        fa = FundamentalAnalysis(sec, "AAPL")
        
        # Test CIK retrieval
        cik = sec.get_cik_from_ticker("AAPL")
        self.assertIsNotNone(cik)
        self.assertEqual(len(cik), 10)
        
        # Test company facts retrieval
        facts = sec.get_company_facts(cik)
        self.assertIsInstance(facts, dict)
        self.assertIn("facts", facts)
        
        # Test financial data retrieval
        fa.fetch_financial_data(metrics_to_retrieve=["revenue", "net_income"])
        self.assertIsNotNone(fa.financial_data)
        self.assertIn("general_info", fa.financial_data)
        
        # Test stock data retrieval
        fa.fetch_stock_data()
        self.assertIsNotNone(fa.stock_data)
        
        # Test market cap calculation
        market_cap = fa.calculate_market_cap()
        self.assertIsInstance(market_cap, dict)
        if market_cap:  # Only test if data is available
            self.assertIn("market_cap", market_cap)
            self.assertGreater(market_cap["market_cap"], 0)


class TestErrorHandling(unittest.TestCase):
    """Test error handling scenarios."""
    
    def setUp(self):
        """Set up error handling test fixtures."""
        self.user_agent = "TestCompany/1.0 (test@example.com)"
        self.sec = SEC(user_agent=self.user_agent)
    
    def test_invalid_ticker(self):
        """Test handling of invalid ticker symbols."""
        with patch.object(self.sec, 'search_cik_manual', return_value=None):
            cik = self.sec.get_cik_from_ticker("INVALID_TICKER_12345")
            self.assertIsNone(cik)
    
    def test_invalid_cik(self):
        """Test handling of invalid CIK."""
        facts = self.sec.get_company_facts("9999999999")
        self.assertEqual(facts, {})
    
    def test_empty_cik(self):
        """Test handling of empty CIK."""
        facts = self.sec.get_company_facts("")
        self.assertEqual(facts, {})
    
    def test_none_cik(self):
        """Test handling of None CIK."""
        facts = self.sec.get_company_facts(None)
        self.assertEqual(facts, {})
    
    @patch('sec.requests.Session.get')
    def test_network_error_handling(self, mock_get):
        """Test handling of network errors."""
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError("Network error")
        
        cik = self.sec.get_cik_from_ticker("AAPL")
        self.assertIsNone(cik)
    
    @patch('sec.requests.Session.get')
    def test_timeout_error_handling(self, mock_get):
        """Test handling of timeout errors."""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout("Request timeout")
        
        cik = self.sec.get_cik_from_ticker("AAPL")
        self.assertIsNone(cik)


class TestUtilityFunctions(unittest.TestCase):
    """Test utility functions."""
    
    def test_create_session_with_retries(self):
        """Test session creation with retry strategy."""
        session = create_session_with_retries()
        self.assertIsNotNone(session)
        
        # Check that adapters are mounted
        self.assertIn("http://", session.adapters)
        self.assertIn("https://", session.adapters)


if __name__ == '__main__':
    # Set up test environment
    os.environ['SEC_USER_AGENT'] = 'TestSuite/1.0 (test@example.com)'
    
    # Configure logging for tests
    import logging
    logging.basicConfig(level=logging.WARNING)  # Reduce log noise during tests
    
    # Run tests
    unittest.main(verbosity=2) 