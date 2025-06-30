#!/usr/bin/env python3
"""
PE Scanner - Detecting Oversold Stocks Using Derived Price-to-Earnings Ratios

This script implements a PE-based stock screening system using the TAI library
for comprehensive financial data analysis. Based on the methodology from:
https://medium.com/@jwu8715/detecting-oversold-stocks-using-derived-price-to-earnings-pe-ratios-223fa490dc8

Key Features:
- Uses SEC data for fundamental analysis (EPS, financial metrics)
- Uses Alpaca API for real-time stock prices
- Calculates various PE ratios and metrics
- Identifies potentially oversold stocks
- Modular design for Jupyter notebook customization

Author: TAI Analytics
Date: 2024
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# TAI Library Imports
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from TAI.source.sec import SEC, FundamentalAnalysis
from TAI.source.alpaca import Alpaca
from TAI.analytics.plotly_plots import QuickPlot, QuantStatsPlot

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PEScanner:
    """
    Advanced PE Scanner for detecting oversold stocks using derived PE ratios.
    
    This class combines SEC fundamental data with real-time market data to identify
    potentially undervalued stocks based on PE analysis.
    """
    
    def __init__(self, use_cache=True, cache_ttl=3600):
        """
        Initialize the PE Scanner with TAI library components.
        
        Args:
            use_cache (bool): Enable caching for SEC data
            cache_ttl (int): Cache time-to-live in seconds
        """
        logger.info("Initializing PE Scanner with TAI library")
        
        # Initialize SEC client for fundamental data
        self.sec = SEC(
            user_agent="PEScanner/1.0 (analytics@tai.com)",
            enable_cache=use_cache,
            cache_ttl=cache_ttl
        )
        
        # Initialize Alpaca client for market data
        try:
            self.alpaca = Alpaca()
            self.market_data_available = True
            logger.info("Alpaca client initialized successfully")
        except Exception as e:
            logger.warning(f"Alpaca client initialization failed: {e}")
            self.market_data_available = False
        
        # Initialize data storage
        self.fundamental_data = {}
        self.market_data = {}
        self.pe_analysis = {}
        
        # Initialize plotting capabilities
        self.plotter = QuickPlot()
        self.quantstats_plotter = QuantStatsPlot()
        
        logger.info("PE Scanner initialized successfully")
    
    def get_fundamental_data(self, ticker: str, lookback_years: int = 5) -> Dict:
        """
        Get comprehensive fundamental data for a ticker using SEC API.
        
        Args:
            ticker (str): Stock ticker symbol
            lookback_years (int): Years of historical data to retrieve
            
        Returns:
            Dict: Fundamental data including EPS, revenue, assets, etc.
        """
        logger.info(f"Fetching fundamental data for {ticker}")
        
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=lookback_years * 365)
            
            # Get financial data using SEC API
            financial_data = self.sec.get_financial_data(
                ticker=ticker,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                metrics_to_retrieve=[
                    'eps_basic', 'eps_diluted', 'revenue', 'net_income',
                    'shares_outstanding', 'total_assets', 'total_liabilities',
                    'equity', 'operating_cash_flow'
                ]
            )
            
            if financial_data and any(key in financial_data for key in ['eps_basic', 'eps_diluted', 'revenue', 'net_income']):
                # Store the raw data
                self.fundamental_data[ticker] = financial_data
                
                # Calculate additional metrics
                processed_data = self._process_fundamental_data(financial_data)
                
                logger.info(f"Successfully retrieved fundamental data for {ticker}")
                return processed_data
            else:
                logger.warning(f"No fundamental data available for {ticker}")
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching fundamental data for {ticker}: {e}")
            return {}
    
    def _process_fundamental_data(self, financial_data: Dict) -> Dict:
        """
        Process raw SEC financial data into usable metrics.
        
        Args:
            financial_data (Dict): Raw financial data from SEC
            
        Returns:
            Dict: Processed fundamental metrics
        """
        # Extract general info
        general_info = financial_data.get('general_info', {})
        
        processed = {
            'company_name': general_info.get('company_name', 'Unknown'),
            'cik': general_info.get('cik', 'Unknown'),
            'currency': 'USD',
            'periods': []
        }
        
        # Get all metric lists
        metric_lists = {}
        for metric in ['eps_basic', 'eps_diluted', 'revenue', 'net_income', 
                      'shares_outstanding', 'total_assets', 'equity', 'operating_cash_flow']:
            metric_lists[metric] = financial_data.get(metric, [])
        
        # Create a unified timeline of periods
        all_periods = set()
        for metric_data in metric_lists.values():
            for item in metric_data:
                period_key = (item.get('end'), item.get('fy'), item.get('fp'))
                all_periods.add(period_key)
        
        # Process each unique period
        for end_date, fy, fp in sorted(all_periods, key=lambda x: (x[0] or '', x[1] or 0, x[2] or '')):
            if not end_date:
                continue
                
            period_data = {
                'period': f"{fy}-{fp}" if fy and fp else 'Unknown',
                'end_date': end_date,
                'fy': fy,
                'fp': fp,
                'eps_basic': None,
                'eps_diluted': None,
                'revenue': None,
                'net_income': None,
                'shares_outstanding': None,
                'total_assets': None,
                'equity': None,
                'operating_cash_flow': None
            }
            
            # Find values for this period across all metrics
            for metric_name, metric_data in metric_lists.items():
                for item in metric_data:
                    if (item.get('end') == end_date and 
                        item.get('fy') == fy and 
                        item.get('fp') == fp):
                        period_data[metric_name] = self._safe_float(item.get('value'))
                        break
            
            processed['periods'].append(period_data)
        
        return processed
    
    def get_current_price(self, ticker: str) -> Optional[float]:
        """
        Get current stock price using Alpaca API.
        
        Args:
            ticker (str): Stock ticker symbol
            
        Returns:
            Optional[float]: Current stock price or None if unavailable
        """
        if not self.market_data_available:
            logger.warning("Market data not available - Alpaca client not initialized")
            return None
        
        try:
            logger.info(f"Fetching current price for {ticker}")
            
            # Get latest quote from Alpaca
            quote_data = self.alpaca.get_latest_quote(ticker)
            
            if quote_data and hasattr(quote_data, 'get'):
                # Extract price from quote data
                if ticker in quote_data:
                    quote = quote_data[ticker]
                    if hasattr(quote, 'bid_price') and hasattr(quote, 'ask_price'):
                        # Use mid-price (average of bid and ask)
                        bid = float(quote.bid_price) if quote.bid_price else 0
                        ask = float(quote.ask_price) if quote.ask_price else 0
                        if bid > 0 and ask > 0:
                            current_price = (bid + ask) / 2
                            logger.info(f"Current price for {ticker}: ${current_price:.2f}")
                            return current_price
            
            # Fallback: try to get latest trade
            trade_data = self.alpaca.get_latest_trade(ticker)
            if trade_data and ticker in trade_data:
                trade = trade_data[ticker]
                if hasattr(trade, 'price'):
                    current_price = float(trade.price)
                    logger.info(f"Current price for {ticker} (trade): ${current_price:.2f}")
                    return current_price
            
            logger.warning(f"Could not retrieve current price for {ticker}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching current price for {ticker}: {e}")
            return None
    
    def calculate_pe_metrics(self, ticker: str) -> Dict:
        """
        Calculate comprehensive PE metrics for a stock.
        
        Args:
            ticker (str): Stock ticker symbol
            
        Returns:
            Dict: PE metrics and analysis
        """
        logger.info(f"Calculating PE metrics for {ticker}")
        
        # Get fundamental data if not already available
        if ticker not in self.fundamental_data:
            fundamental_data = self.get_fundamental_data(ticker)
        else:
            fundamental_data = self.fundamental_data[ticker]
        
        # Get current price
        current_price = self.get_current_price(ticker)
        if not current_price:
            logger.warning(f"Cannot calculate PE metrics for {ticker} - no current price")
            return {}
        
        if not fundamental_data or not fundamental_data.get('periods'):
            logger.warning(f"Cannot calculate PE metrics for {ticker} - no fundamental data")
            return {}
        
        try:
            periods = fundamental_data['periods']
            
            # Calculate various PE metrics
            pe_metrics = {
                'ticker': ticker,
                'current_price': current_price,
                'company_name': fundamental_data.get('company_name', 'Unknown'),
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'pe_ratios': {},
                'eps_growth': {},
                'valuation_scores': {}
            }
            
            # Current period metrics
            if periods:
                latest_period = periods[-1]  # Most recent period
                
                # Current PE Ratios
                if latest_period['eps_basic']:
                    pe_metrics['pe_ratios']['current_pe_basic'] = current_price / latest_period['eps_basic']
                
                if latest_period['eps_diluted']:
                    pe_metrics['pe_ratios']['current_pe_diluted'] = current_price / latest_period['eps_diluted']
                
                # TTM (Trailing Twelve Months) PE
                ttm_eps = self._calculate_ttm_eps(periods)
                if ttm_eps:
                    pe_metrics['pe_ratios']['ttm_pe'] = current_price / ttm_eps
                    pe_metrics['ttm_eps'] = ttm_eps
                
                # Forward PE estimate (simple projection)
                eps_growth_rate = self._calculate_eps_growth_rate(periods)
                if ttm_eps and eps_growth_rate is not None:
                    forward_eps = ttm_eps * (1 + eps_growth_rate)
                    pe_metrics['pe_ratios']['forward_pe'] = current_price / forward_eps if forward_eps > 0 else None
                    pe_metrics['eps_growth']['annual_growth_rate'] = eps_growth_rate
                
                # Historical PE analysis
                historical_pe = self._calculate_historical_pe_stats(periods, current_price)
                pe_metrics['historical_analysis'] = historical_pe
                
                # Valuation scores
                pe_metrics['valuation_scores'] = self._calculate_valuation_scores(pe_metrics)
                
                # Store the analysis
                self.pe_analysis[ticker] = pe_metrics
                
                logger.info(f"PE metrics calculated successfully for {ticker}")
                return pe_metrics
            
        except Exception as e:
            logger.error(f"Error calculating PE metrics for {ticker}: {e}")
            return {}
    
    def _calculate_ttm_eps(self, periods: List[Dict]) -> Optional[float]:
        """Calculate trailing twelve months EPS."""
        if len(periods) >= 4:
            # Sum last 4 quarters for TTM
            recent_quarters = periods[-4:]
            ttm_eps = sum([p.get('eps_basic', 0) or 0 for p in recent_quarters])
            return ttm_eps if ttm_eps > 0 else None
        elif periods:
            # If less than 4 quarters, use most recent annual data
            return periods[-1].get('eps_basic')
        return None
    
    def _calculate_eps_growth_rate(self, periods: List[Dict]) -> Optional[float]:
        """Calculate EPS growth rate."""
        if len(periods) >= 2:
            recent_eps = periods[-1].get('eps_basic', 0) or 0
            previous_eps = periods[-2].get('eps_basic', 0) or 0
            
            if previous_eps > 0:
                growth_rate = (recent_eps - previous_eps) / previous_eps
                return growth_rate
        return None
    
    def _calculate_historical_pe_stats(self, periods: List[Dict], current_price: float) -> Dict:
        """Calculate historical PE statistics."""
        historical_pes = []
        
        for period in periods:
            eps = period.get('eps_basic')
            if eps and eps > 0:
                pe = current_price / eps
                historical_pes.append(pe)
        
        if historical_pes:
            return {
                'avg_pe': np.mean(historical_pes),
                'median_pe': np.median(historical_pes),
                'min_pe': np.min(historical_pes),
                'max_pe': np.max(historical_pes),
                'std_pe': np.std(historical_pes),
                'pe_percentile_25': np.percentile(historical_pes, 25),
                'pe_percentile_75': np.percentile(historical_pes, 75)
            }
        return {}
    
    def _calculate_valuation_scores(self, pe_metrics: Dict) -> Dict:
        """Calculate valuation scores based on PE analysis."""
        scores = {}
        
        # PE Score (lower is better)
        current_pe = pe_metrics['pe_ratios'].get('current_pe_basic')
        historical = pe_metrics.get('historical_analysis', {})
        
        if current_pe and historical:
            avg_pe = historical.get('avg_pe')
            if avg_pe:
                pe_score = current_pe / avg_pe  # Score < 1 means undervalued
                scores['pe_relative_score'] = pe_score
                scores['is_undervalued_pe'] = pe_score < 0.8  # 20% below historical average
        
        # Growth-adjusted PE score
        eps_growth = pe_metrics['eps_growth'].get('annual_growth_rate')
        if current_pe and eps_growth:
            peg_ratio = current_pe / (eps_growth * 100) if eps_growth > 0 else None
            scores['peg_ratio'] = peg_ratio
            scores['is_undervalued_peg'] = peg_ratio < 1.0 if peg_ratio else False
        
        return scores
    
    def screen_stocks(self, tickers: List[str], criteria: Dict = None) -> pd.DataFrame:
        """
        Screen multiple stocks based on PE criteria.
        
        Args:
            tickers (List[str]): List of stock ticker symbols
            criteria (Dict): Screening criteria
            
        Returns:
            pd.DataFrame: Screening results
        """
        logger.info(f"Screening {len(tickers)} stocks for PE opportunities")
        
        # Default screening criteria
        default_criteria = {
            'max_pe': 25,
            'min_pe': 5,
            'min_market_cap': 1e9,  # $1B minimum
            'require_positive_eps': True,
            'max_peg_ratio': 1.5,
            'undervalued_threshold': 0.8
        }
        
        if criteria:
            default_criteria.update(criteria)
        
        results = []
        
        for ticker in tickers:
            try:
                logger.info(f"Analyzing {ticker}")
                
                # Calculate PE metrics
                pe_data = self.calculate_pe_metrics(ticker)
                
                if pe_data:
                    # Apply screening criteria
                    screen_result = self._apply_screening_criteria(pe_data, default_criteria)
                    results.append(screen_result)
                    
            except Exception as e:
                logger.error(f"Error screening {ticker}: {e}")
                continue
        
        # Convert to DataFrame
        if results:
            df = pd.DataFrame(results)
            
            # Sort by valuation attractiveness
            if 'pe_relative_score' in df.columns:
                df = df.sort_values('pe_relative_score', ascending=True)
            
            logger.info(f"Screening completed. Found {len(df)} results")
            return df
        else:
            logger.warning("No results from screening")
            return pd.DataFrame()
    
    def _apply_screening_criteria(self, pe_data: Dict, criteria: Dict) -> Dict:
        """Apply screening criteria to PE data."""
        result = {
            'ticker': pe_data['ticker'],
            'company_name': pe_data['company_name'],
            'current_price': pe_data['current_price'],
            'current_pe': pe_data['pe_ratios'].get('current_pe_basic'),
            'ttm_pe': pe_data['pe_ratios'].get('ttm_pe'),
            'forward_pe': pe_data['pe_ratios'].get('forward_pe'),
            'peg_ratio': pe_data['valuation_scores'].get('peg_ratio'),
            'pe_relative_score': pe_data['valuation_scores'].get('pe_relative_score'),
            'eps_growth_rate': pe_data['eps_growth'].get('annual_growth_rate'),
            'ttm_eps': pe_data.get('ttm_eps'),
            'passes_screen': True,
            'screen_reasons': []
        }
        
        # Apply screening criteria
        current_pe = result['current_pe']
        
        if current_pe:
            if current_pe > criteria['max_pe']:
                result['passes_screen'] = False
                result['screen_reasons'].append(f"PE too high: {current_pe:.2f}")
            
            if current_pe < criteria['min_pe']:
                result['passes_screen'] = False
                result['screen_reasons'].append(f"PE too low: {current_pe:.2f}")
        
        if criteria['require_positive_eps'] and (not result['ttm_eps'] or result['ttm_eps'] <= 0):
            result['passes_screen'] = False
            result['screen_reasons'].append("Negative or zero EPS")
        
        peg_ratio = result['peg_ratio']
        if peg_ratio and peg_ratio > criteria['max_peg_ratio']:
            result['passes_screen'] = False
            result['screen_reasons'].append(f"PEG ratio too high: {peg_ratio:.2f}")
        
        # Mark potentially undervalued stocks
        pe_score = result['pe_relative_score']
        if pe_score and pe_score < criteria['undervalued_threshold']:
            result['potentially_undervalued'] = True
        else:
            result['potentially_undervalued'] = False
        
        return result
    
    def generate_analysis_report(self, ticker: str) -> str:
        """
        Generate a comprehensive analysis report for a ticker.
        
        Args:
            ticker (str): Stock ticker symbol
            
        Returns:
            str: Formatted analysis report
        """
        if ticker not in self.pe_analysis:
            self.calculate_pe_metrics(ticker)
        
        pe_data = self.pe_analysis.get(ticker, {})
        
        if not pe_data:
            return f"No analysis data available for {ticker}"
        
        # Format values properly to avoid f-string format errors
        current_pe_basic = pe_data['pe_ratios'].get('current_pe_basic')
        ttm_pe = pe_data['pe_ratios'].get('ttm_pe')
        forward_pe = pe_data['pe_ratios'].get('forward_pe')
        ttm_eps = pe_data.get('ttm_eps')
        growth_rate = pe_data['eps_growth'].get('annual_growth_rate')
        peg_ratio = pe_data['valuation_scores'].get('peg_ratio')
        pe_relative_score = pe_data['valuation_scores'].get('pe_relative_score')
        
        current_pe_str = f"{current_pe_basic:.2f}" if current_pe_basic is not None else 'N/A'
        ttm_pe_str = f"{ttm_pe:.2f}" if ttm_pe is not None else 'N/A'
        forward_pe_str = f"{forward_pe:.2f}" if forward_pe is not None else 'N/A'
        ttm_eps_str = f"${ttm_eps:.2f}" if ttm_eps is not None else 'N/A'
        growth_rate_str = f"{growth_rate:.2%}" if growth_rate is not None else 'N/A'
        peg_ratio_str = f"{peg_ratio:.2f}" if peg_ratio is not None else 'N/A'
        pe_relative_score_str = f"{pe_relative_score:.2f}" if pe_relative_score is not None else 'N/A'
        
        report = f"""
=== PE ANALYSIS REPORT FOR {ticker} ===
Company: {pe_data.get('company_name', 'Unknown')}
Analysis Date: {pe_data.get('analysis_date', 'Unknown')}
Current Price: ${pe_data.get('current_price', 0):.2f}

PE RATIOS:
- Current PE (Basic): {current_pe_str}
- TTM PE: {ttm_pe_str}
- Forward PE: {forward_pe_str}

EARNINGS:
- TTM EPS: {ttm_eps_str}
- EPS Growth Rate: {growth_rate_str}

VALUATION METRICS:
- PEG Ratio: {peg_ratio_str}
- PE Relative Score: {pe_relative_score_str}
- Undervalued (PE): {pe_data['valuation_scores'].get('is_undervalued_pe', 'N/A')}
- Undervalued (PEG): {pe_data['valuation_scores'].get('is_undervalued_peg', 'N/A')}

HISTORICAL PE ANALYSIS:
"""
        
        historical = pe_data.get('historical_analysis', {})
        if historical:
            avg_pe = historical.get('avg_pe')
            median_pe = historical.get('median_pe')
            min_pe = historical.get('min_pe')
            max_pe = historical.get('max_pe')
            pe_25 = historical.get('pe_percentile_25')
            pe_75 = historical.get('pe_percentile_75')
            
            avg_pe_str = f"{avg_pe:.2f}" if avg_pe is not None else 'N/A'
            median_pe_str = f"{median_pe:.2f}" if median_pe is not None else 'N/A'
            min_pe_str = f"{min_pe:.2f}" if min_pe is not None else 'N/A'
            max_pe_str = f"{max_pe:.2f}" if max_pe is not None else 'N/A'
            pe_25_str = f"{pe_25:.2f}" if pe_25 is not None else 'N/A'
            pe_75_str = f"{pe_75:.2f}" if pe_75 is not None else 'N/A'
            
            report += f"""- Average PE: {avg_pe_str}
- Median PE: {median_pe_str}
- PE Range: {min_pe_str} - {max_pe_str}
- PE 25th Percentile: {pe_25_str}
- PE 75th Percentile: {pe_75_str}
"""
        
        return report
    
    @staticmethod
    def _safe_float(value) -> Optional[float]:
        """Safely convert value to float."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    # =============================================================================
    # PLOTTING AND VISUALIZATION METHODS
    # =============================================================================
    
    def plot_pe_comparison(self, tickers: List[str], metrics: List[str] = None, title: str = "PE Ratio Comparison"):
        """
        Plot PE ratio comparison across multiple stocks.
        
        Args:
            tickers (List[str]): List of stock tickers to compare
            metrics (List[str]): PE metrics to plot ['current_pe', 'ttm_pe', 'forward_pe']
            title (str): Plot title
            
        Returns:
            plotly.graph_objects.Figure: Interactive bar chart
        """
        if metrics is None:
            metrics = ['current_pe_basic', 'ttm_pe']
        
        # Collect data for all tickers
        plot_data = []
        
        for ticker in tickers:
            if ticker not in self.pe_analysis:
                self.calculate_pe_metrics(ticker)
            
            pe_data = self.pe_analysis.get(ticker)
            if pe_data:
                row = {'ticker': ticker, 'company': pe_data.get('company_name', ticker)}
                
                for metric in metrics:
                    if metric == 'current_pe_basic':
                        row['Current PE'] = pe_data['pe_ratios'].get('current_pe_basic')
                    elif metric == 'ttm_pe':
                        row['TTM PE'] = pe_data['pe_ratios'].get('ttm_pe')
                    elif metric == 'forward_pe':
                        row['Forward PE'] = pe_data['pe_ratios'].get('forward_pe')
                
                plot_data.append(row)
        
        if not plot_data:
            print("No data available for plotting")
            return None
        
        # Create DataFrame for plotting
        df = pd.DataFrame(plot_data)
        
        # Create bar plot using QuickPlot
        import plotly.graph_objects as go
        fig = go.Figure()
        
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
        
        for i, metric in enumerate([col for col in df.columns if col not in ['ticker', 'company']]):
            fig.add_trace(go.Bar(
                name=metric,
                x=df['ticker'],
                y=df[metric],
                marker_color=colors[i % len(colors)],
                text=df[metric].round(2),
                textposition='auto'
            ))
        
        fig.update_layout(
            title=title,
            xaxis_title='Stock',
            yaxis_title='PE Ratio',
            barmode='group',
            hovermode='x unified'
        )
        
        return fig
    
    def plot_historical_pe_trend(self, ticker: str, current_price: float = None):
        """
        Plot historical PE trend for a single stock.
        
        Args:
            ticker (str): Stock ticker symbol
            current_price (float): Current stock price (if None, will fetch)
            
        Returns:
            plotly.graph_objects.Figure: Line plot of PE ratios over time
        """
        if ticker not in self.fundamental_data:
            self.get_fundamental_data(ticker)
        
        fundamental_data = self.fundamental_data.get(ticker)
        if not fundamental_data or not fundamental_data.get('periods'):
            print(f"No fundamental data available for {ticker}")
            return None
        
        if current_price is None:
            current_price = self.get_current_price(ticker)
            if not current_price:
                print(f"Could not get current price for {ticker}")
                return None
        
        # Prepare data for plotting
        periods = fundamental_data['periods']
        dates = []
        pe_ratios = []
        
        for period in periods:
            if period.get('end_date') and period.get('eps_basic') and period['eps_basic'] > 0:
                try:
                    date = pd.to_datetime(period['end_date'])
                    pe_ratio = current_price / period['eps_basic']
                    dates.append(date)
                    pe_ratios.append(pe_ratio)
                except:
                    continue
        
        if not dates:
            print(f"No valid PE data for plotting {ticker}")
            return None
        
        # Create DataFrame for plotting
        df = pd.DataFrame({
            'date': dates,
            'pe_ratio': pe_ratios
        })
        
        # Use QuickPlot to create line chart
        fig = self.plotter.plot_lines(
            dfs=[df], 
            labels=[f'{ticker} PE Ratio'],
            title=f'{ticker} - Historical PE Ratio Trend'
        )
        
        # Add statistical lines
        import plotly.graph_objects as go
        mean_pe = np.mean(pe_ratios)
        fig.add_hline(y=mean_pe, line_dash="dash", line_color="red", 
                     annotation_text=f"Average PE: {mean_pe:.2f}")
        
        return fig
    
    def plot_pe_vs_growth_scatter(self, screening_results: pd.DataFrame):
        """
        Create scatter plot of PE ratio vs EPS growth rate.
        
        Args:
            screening_results (pd.DataFrame): Results from screen_stocks method
            
        Returns:
            plotly.graph_objects.Figure: Scatter plot
        """
        if screening_results.empty:
            print("No screening results to plot")
            return None
        
        # Filter for valid data
        valid_data = screening_results.dropna(subset=['current_pe', 'eps_growth_rate'])
        
        if valid_data.empty:
            print("No valid PE and growth data for plotting")
            return None
        
        import plotly.graph_objects as go
        import plotly.express as px
        
        fig = go.Figure()
        
        # Color by undervaluation status
        colors = ['green' if uv else 'blue' for uv in valid_data['potentially_undervalued']]
        
        fig.add_trace(go.Scatter(
            x=valid_data['eps_growth_rate'] * 100,  # Convert to percentage
            y=valid_data['current_pe'],
            mode='markers+text',
            text=valid_data['ticker'],
            textposition='top center',
            marker=dict(
                color=colors,
                size=10,
                opacity=0.7
            ),
            hovertemplate='<b>%{text}</b><br>' +
                         'PE Ratio: %{y:.2f}<br>' +
                         'EPS Growth: %{x:.1f}%<br>' +
                         '<extra></extra>'
        ))
        
        fig.update_layout(
            title='PE Ratio vs EPS Growth Rate',
            xaxis_title='EPS Growth Rate (%)',
            yaxis_title='PE Ratio',
            showlegend=False
        )
        
        # Add PEG ratio reference lines
        for peg in [1.0, 1.5, 2.0]:
            x_range = np.linspace(valid_data['eps_growth_rate'].min() * 100, 
                                valid_data['eps_growth_rate'].max() * 100, 100)
            y_peg_line = peg * x_range
            
            fig.add_trace(go.Scatter(
                x=x_range,
                y=y_peg_line,
                mode='lines',
                line=dict(dash='dot', color='gray'),
                name=f'PEG = {peg}',
                opacity=0.5
            ))
        
        return fig
    
    def plot_sector_pe_heatmap(self, sector_results: Dict):
        """
        Create heatmap showing PE ratios across sectors.
        
        Args:
            sector_results (Dict): Results from sector analysis
            
        Returns:
            plotly.graph_objects.Figure: Heatmap
        """
        if not sector_results:
            print("No sector results to plot")
            return None
        
        # Prepare data for heatmap
        heatmap_data = []
        
        for sector, data in sector_results.items():
            if 'data' in data and not data['data'].empty:
                sector_df = data['data']
                for _, row in sector_df.iterrows():
                    heatmap_data.append({
                        'Sector': sector,
                        'Ticker': row['ticker'],
                        'PE_Ratio': row['current_pe'],
                        'Undervalued': row['potentially_undervalued']
                    })
        
        if not heatmap_data:
            print("No valid data for heatmap")
            return None
        
        df = pd.DataFrame(heatmap_data)
        
        # Create pivot table for heatmap
        pivot_df = df.pivot(index='Sector', columns='Ticker', values='PE_Ratio')
        
        import plotly.graph_objects as go
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot_df.values,
            x=pivot_df.columns,
            y=pivot_df.index,
            colorscale='RdYlBu_r',
            hoverongaps=False,
            text=pivot_df.values.round(2),
            texttemplate='%{text}',
            textfont={'size': 10}
        ))
        
        fig.update_layout(
            title='PE Ratios by Sector and Stock',
            xaxis_title='Stock',
            yaxis_title='Sector'
        )
        
        return fig
    
    def plot_valuation_dashboard(self, ticker: str):
        """
        Create a comprehensive valuation dashboard for a single stock.
        
        Args:
            ticker (str): Stock ticker symbol
            
        Returns:
            plotly.graph_objects.Figure: Subplot dashboard
        """
        if ticker not in self.pe_analysis:
            self.calculate_pe_metrics(ticker)
        
        pe_data = self.pe_analysis.get(ticker)
        if not pe_data:
            print(f"No analysis data available for {ticker}")
            return None
        
        from plotly.subplots import make_subplots
        import plotly.graph_objects as go
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                f'{ticker} - PE Ratios',
                f'{ticker} - Historical PE Stats',
                f'{ticker} - Valuation Scores',
                f'{ticker} - Key Metrics'
            ),
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "indicator"}, {"type": "table"}]]
        )
        
        # PE Ratios Bar Chart
        pe_ratios = pe_data['pe_ratios']
        pe_names = []
        pe_values = []
        
        for name, value in pe_ratios.items():
            if value is not None:
                pe_names.append(name.replace('_', ' ').title())
                pe_values.append(value)
        
        fig.add_trace(
            go.Bar(x=pe_names, y=pe_values, name='PE Ratios', marker_color='lightblue'),
            row=1, col=1
        )
        
        # Historical PE Stats
        historical = pe_data.get('historical_analysis', {})
        if historical:
            hist_names = ['Avg PE', 'Median PE', 'Min PE', 'Max PE']
            hist_values = [
                historical.get('avg_pe', 0),
                historical.get('median_pe', 0),
                historical.get('min_pe', 0),
                historical.get('max_pe', 0)
            ]
            
            fig.add_trace(
                go.Bar(x=hist_names, y=hist_values, name='Historical PE', marker_color='lightgreen'),
                row=1, col=2
            )
        
        # Valuation Score Gauge
        pe_score = pe_data['valuation_scores'].get('pe_relative_score', 1)
        fig.add_trace(
            go.Indicator(
                mode="gauge+number+delta",
                value=pe_score,
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "PE Relative Score"},
                gauge={
                    'axis': {'range': [None, 2]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 0.8], 'color': "lightgreen"},
                        {'range': [0.8, 1.2], 'color': "yellow"},
                        {'range': [1.2, 2], 'color': "lightcoral"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 1.0
                    }
                }
            ),
            row=2, col=1
        )
        
        # Key Metrics Table
        table_data = [
            ['Current Price', f"${pe_data.get('current_price', 0):.2f}"],
            ['Company', pe_data.get('company_name', 'Unknown')],
            ['TTM EPS', f"${pe_data.get('ttm_eps', 0):.2f}"],
            ['PEG Ratio', f"{pe_data['valuation_scores'].get('peg_ratio', 0):.2f}" if pe_data['valuation_scores'].get('peg_ratio') else 'N/A'],
            ['Undervalued (PE)', str(pe_data['valuation_scores'].get('is_undervalued_pe', False))],
            ['Analysis Date', pe_data.get('analysis_date', 'Unknown')]
        ]
        
        fig.add_trace(
            go.Table(
                header=dict(values=['Metric', 'Value']),
                cells=dict(values=[[row[0] for row in table_data], 
                                 [row[1] for row in table_data]])
            ),
            row=2, col=2
        )
        
        fig.update_layout(
            height=800,
            title_text=f"{ticker} - Comprehensive Valuation Dashboard",
            showlegend=False
        )
        
        return fig

# =============================================================================
# JUPYTER NOTEBOOK EXECUTION BLOCKS
# =============================================================================

def block_1_initialize_scanner():
    """
    Block 1: Initialize the PE Scanner
    Run this first to set up the scanner with TAI library components.
    """
    print("=== BLOCK 1: Initializing PE Scanner ===")
    
    scanner = PEScanner(use_cache=True, cache_ttl=3600)
    
    print("✅ PE Scanner initialized with:")
    print("   • SEC API for fundamental data")
    print("   • Alpaca API for market data")
    print("   • Intelligent caching enabled")
    
    return scanner

def block_2_analyze_single_stock(scanner, ticker="AAPL"):
    """
    Block 2: Analyze a single stock
    Customize the ticker symbol to analyze different stocks.
    """
    print(f"=== BLOCK 2: Analyzing {ticker} ===")
    
    # Get comprehensive PE analysis
    pe_metrics = scanner.calculate_pe_metrics(ticker)
    
    if pe_metrics:
        print(f"✅ Analysis completed for {ticker}")
        
        # Display key metrics
        current_pe = pe_metrics['pe_ratios'].get('current_pe_basic')
        ttm_pe = pe_metrics['pe_ratios'].get('ttm_pe')
        peg = pe_metrics['valuation_scores'].get('peg_ratio')
        
        # Format values properly
        current_pe_str = f"{current_pe:.2f}" if current_pe is not None else 'N/A'
        ttm_pe_str = f"{ttm_pe:.2f}" if ttm_pe is not None else 'N/A'
        peg_str = f"{peg:.2f}" if peg is not None else 'N/A'
        
        print(f"   Current Price: ${pe_metrics['current_price']:.2f}")
        print(f"   Current PE: {current_pe_str}")
        print(f"   TTM PE: {ttm_pe_str}")
        print(f"   PEG Ratio: {peg_str}")
        
        # Generate detailed report
        report = scanner.generate_analysis_report(ticker)
        print("\n" + report)
        
        return pe_metrics
    else:
        print(f"❌ Could not analyze {ticker}")
        return None

def block_3_screen_multiple_stocks(scanner, tickers=None, custom_criteria=None):
    """
    Block 3: Screen multiple stocks
    Customize the ticker list and screening criteria.
    """
    print("=== BLOCK 3: Multi-Stock PE Screening ===")
    
    # Default ticker list (customize as needed)
    if tickers is None:
        tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'JNJ', 'WMT']
    
    # Default screening criteria (customize as needed)
    if custom_criteria is None:
        custom_criteria = {
            'max_pe': 25,
            'min_pe': 5,
            'require_positive_eps': True,
            'max_peg_ratio': 1.5,
            'undervalued_threshold': 0.8
        }
    
    print(f"Screening {len(tickers)} stocks with criteria:")
    for key, value in custom_criteria.items():
        print(f"   {key}: {value}")
    
    # Run the screening
    results = scanner.screen_stocks(tickers, custom_criteria)
    
    if not results.empty:
        print(f"\n✅ Screening completed. Results for {len(results)} stocks:")
        
        # Display summary
        passed_screen = results[results['passes_screen'] == True]
        undervalued = results[results['potentially_undervalued'] == True]
        
        print(f"   Stocks passing screen: {len(passed_screen)}")
        print(f"   Potentially undervalued: {len(undervalued)}")
        
        # Show top candidates
        if not undervalued.empty:
            print("\n🎯 Top undervalued candidates:")
            top_candidates = undervalued.head(5)[['ticker', 'company_name', 'current_pe', 'pe_relative_score', 'peg_ratio']]
            print(top_candidates.to_string(index=False))
        
        return results
    else:
        print("❌ No screening results")
        return pd.DataFrame()

def block_4_custom_analysis(scanner):
    """
    Block 4: Custom analysis and visualization
    Add your own custom analysis here.
    """
    print("=== BLOCK 4: Custom Analysis ===")
    
    # Example: Compare multiple stocks
    comparison_tickers = ['AAPL', 'MSFT', 'GOOGL']
    comparison_data = []
    
    for ticker in comparison_tickers:
        pe_data = scanner.calculate_pe_metrics(ticker)
        if pe_data:
            comparison_data.append({
                'Ticker': ticker,
                'Company': pe_data['company_name'],
                'Price': pe_data['current_price'],
                'PE Ratio': pe_data['pe_ratios'].get('current_pe_basic'),
                'PEG Ratio': pe_data['valuation_scores'].get('peg_ratio'),
                'PE Score': pe_data['valuation_scores'].get('pe_relative_score')
            })
    
    if comparison_data:
        comparison_df = pd.DataFrame(comparison_data)
        print("📊 Stock Comparison:")
        print(comparison_df.to_string(index=False))
        
        return comparison_df
    
    return None

def block_5_visualization_examples(scanner, screening_results=None):
    """
    Block 5: Visualization Examples
    Demonstrate various plotting capabilities for PE analysis.
    """
    print("=== BLOCK 5: PE Analysis Visualizations ===")
    
    # Example tickers for plotting
    plot_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']
    
    print("📊 Available plotting methods:")
    print("   1. PE Ratio Comparison Chart")
    print("   2. Historical PE Trend")
    print("   3. PE vs Growth Scatter Plot")
    print("   4. Comprehensive Valuation Dashboard")
    print("   5. Sector PE Heatmap")
    
    # Example 1: PE Ratio Comparison
    print("\n🔍 Creating PE ratio comparison chart...")
    comparison_fig = scanner.plot_pe_comparison(
        tickers=plot_tickers,
        title="Tech Stocks - PE Ratio Comparison"
    )
    
    # Example 2: Historical PE Trend for AAPL
    print("📈 Creating historical PE trend for AAPL...")
    trend_fig = scanner.plot_historical_pe_trend('AAPL')
    
    # Example 3: Valuation Dashboard for MSFT
    print("📋 Creating valuation dashboard for MSFT...")
    dashboard_fig = scanner.plot_valuation_dashboard('MSFT')
    
    # Example 4: PE vs Growth Scatter (if screening results available)
    scatter_fig = None
    if screening_results is not None and not screening_results.empty:
        print("🎯 Creating PE vs Growth scatter plot...")
        scatter_fig = scanner.plot_pe_vs_growth_scatter(screening_results)
    
    print("\n✅ Visualization examples created!")
    print("💡 Usage tips:")
    print("   - All plots are interactive Plotly figures")
    print("   - Use fig.show() to display in Jupyter")
    print("   - Use fig.write_html('filename.html') to save")
    print("   - Hover over points for detailed information")
    
    return {
        'pe_comparison': comparison_fig,
        'pe_trend': trend_fig,
        'dashboard': dashboard_fig,
        'scatter': scatter_fig
    }

def jupyter_plotting_tutorial():
    """
    Tutorial function showing how to use plotting in Jupyter notebooks.
    """
    tutorial_code = '''
# =============================================================================
# PE SCANNER PLOTTING TUTORIAL - Copy these cells to your Jupyter notebook
# =============================================================================

# CELL 1: Basic Setup
import sys
sys.path.append('path/to/TAI')  # Adjust path as needed
from pe_scanner import PEScanner

scanner = PEScanner()

# CELL 2: Single Stock Analysis with Plot
ticker = 'AAPL'
pe_data = scanner.calculate_pe_metrics(ticker)

# Create and display valuation dashboard
dashboard = scanner.plot_valuation_dashboard(ticker)
dashboard.show()

# CELL 3: Multi-Stock PE Comparison
tickers = ['AAPL', 'MSFT', 'GOOGL', 'TSLA']
comparison_chart = scanner.plot_pe_comparison(
    tickers=tickers,
    title="Tech Giants - PE Comparison"
)
comparison_chart.show()

# CELL 4: Historical PE Trend Analysis  
trend_plot = scanner.plot_historical_pe_trend('AAPL')
trend_plot.show()

# CELL 5: Screening with Visualization
screening_results = scanner.screen_stocks(
    tickers=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA'],
    criteria={'max_pe': 30, 'undervalued_threshold': 0.85}
)

# Plot PE vs Growth relationship
scatter_plot = scanner.plot_pe_vs_growth_scatter(screening_results)
if scatter_plot:
    scatter_plot.show()

# CELL 6: Sector Analysis with Heatmap
sectors = {
    'Technology': ['AAPL', 'MSFT', 'GOOGL'],
    'Healthcare': ['JNJ', 'PFE', 'UNH'],
    'Financial': ['JPM', 'BAC', 'GS']
}

sector_results = {}
for sector_name, sector_tickers in sectors.items():
    results = scanner.screen_stocks(sector_tickers)
    if not results.empty:
        sector_results[sector_name] = {'data': results}

# Create sector heatmap
heatmap = scanner.plot_sector_pe_heatmap(sector_results)
if heatmap:
    heatmap.show()

# CELL 7: Save Plots
dashboard.write_html('pe_dashboard.html')
comparison_chart.write_html('pe_comparison.html')
print("✅ Plots saved to HTML files!")
'''
    
    print("📚 JUPYTER PLOTTING TUTORIAL")
    print("=" * 50)
    print("Copy the following code blocks to your Jupyter notebook:")
    print(tutorial_code)
    
    return tutorial_code

# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    """
    Example usage - can be run as a script or imported into Jupyter notebook
    """
    
    print("🚀 PE Scanner - Detecting Oversold Stocks Using Derived PE Ratios")
    print("=" * 70)
    
    # Block 1: Initialize
    scanner = block_1_initialize_scanner()
    
    # Block 2: Single stock analysis
    pe_data = block_2_analyze_single_stock(scanner, "AAPL")
    
    # Block 3: Multi-stock screening
    screening_results = block_3_screen_multiple_stocks(scanner)
    
    # Block 4: Custom analysis
    comparison = block_4_custom_analysis(scanner)
    
    # Block 5: Visualization examples
    visualization_results = block_5_visualization_examples(scanner, screening_results)
    
    print("\n🎉 PE Scanner analysis completed!")
    print("=" * 70)
