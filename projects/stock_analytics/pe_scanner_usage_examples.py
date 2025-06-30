#!/usr/bin/env python3
"""
PE Scanner Usage Examples
========================

This file demonstrates various ways to use the PE Scanner for stock analysis.
Perfect for copying into Jupyter notebook cells or running as standalone scripts.

Examples included:
1. Basic single stock analysis
2. Multi-stock screening
3. Custom criteria screening  
4. Sector analysis
5. Jupyter notebook workflow
"""

import sys
import os
# Add the project root to Python path for TAI library
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from pe_scanner import PEScanner, block_1_initialize_scanner, block_2_analyze_single_stock, block_3_screen_multiple_stocks, block_4_custom_analysis
import pandas as pd

def example_1_basic_analysis():
    """
    Example 1: Basic single stock analysis
    Perfect for getting started or analyzing a specific stock
    """
    print("=" * 60)
    print("EXAMPLE 1: Basic Single Stock Analysis")
    print("=" * 60)
    
    # Initialize scanner
    scanner = PEScanner()
    
    # Analyze Apple
    ticker = "AAPL"
    print(f"\n🔍 Analyzing {ticker}...")
    
    pe_data = scanner.calculate_pe_metrics(ticker)
    
    if pe_data:
        # Print key metrics
        print(f"\n📊 Key Metrics for {ticker}:")
        print(f"   Company: {pe_data['company_name']}")
        print(f"   Current Price: ${pe_data['current_price']:.2f}")
        current_pe = pe_data['pe_ratios'].get('current_pe_basic')
        ttm_pe = pe_data['pe_ratios'].get('ttm_pe')
        peg_ratio = pe_data['valuation_scores'].get('peg_ratio')
        
        # Format values properly
        current_pe_str = f"{current_pe:.2f}" if current_pe is not None else 'N/A'
        ttm_pe_str = f"{ttm_pe:.2f}" if ttm_pe is not None else 'N/A'
        peg_ratio_str = f"{peg_ratio:.2f}" if peg_ratio is not None else 'N/A'
        
        print(f"   Current PE: {current_pe_str}")
        print(f"   TTM PE: {ttm_pe_str}")
        print(f"   PEG Ratio: {peg_ratio_str}")
        
        # Generate full report
        report = scanner.generate_analysis_report(ticker)
        print(report)
    
    return scanner

def example_2_multi_stock_screening():
    """
    Example 2: Screen multiple stocks for opportunities
    """
    print("=" * 60)
    print("EXAMPLE 2: Multi-Stock Screening")
    print("=" * 60)
    
    scanner = PEScanner()
    
    # Define stock universe
    tech_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA']
    
    print(f"🔍 Screening {len(tech_stocks)} tech stocks...")
    
    # Run screening with default criteria
    results = scanner.screen_stocks(tech_stocks)
    
    if not results.empty:
        print(f"\n📊 Screening Results:")
        print(f"   Total analyzed: {len(results)}")
        print(f"   Passed screen: {len(results[results['passes_screen']])}")
        print(f"   Potentially undervalued: {len(results[results['potentially_undervalued']])}")
        
        # Show best opportunities
        undervalued = results[results['potentially_undervalued'] == True]
        if not undervalued.empty:
            print("\n🎯 Best Opportunities:")
            display_cols = ['ticker', 'company_name', 'current_pe', 'pe_relative_score', 'peg_ratio']
            print(undervalued[display_cols].to_string(index=False))
        
        # Show all results summary
        print("\n📋 All Results Summary:")
        summary_cols = ['ticker', 'current_pe', 'ttm_pe', 'peg_ratio', 'passes_screen', 'potentially_undervalued']
        print(results[summary_cols].to_string(index=False))
    
    return results

def example_3_custom_screening_criteria():
    """
    Example 3: Custom screening criteria for different strategies
    """
    print("=" * 60)
    print("EXAMPLE 3: Custom Screening Criteria")
    print("=" * 60)
    
    scanner = PEScanner()
    
    # Define different screening strategies
    
    # Strategy 1: Conservative Value Investing
    conservative_criteria = {
        'max_pe': 15,          # Low PE requirement
        'min_pe': 8,           # Avoid distressed stocks
        'require_positive_eps': True,
        'max_peg_ratio': 1.0,  # Strong growth requirement
        'undervalued_threshold': 0.7  # 30% below historical average
    }
    
    # Strategy 2: Growth at Reasonable Price (GARP)
    garp_criteria = {
        'max_pe': 30,          # Allow higher PE for growth
        'min_pe': 10,
        'require_positive_eps': True,
        'max_peg_ratio': 1.5,  # Focus on PEG ratio
        'undervalued_threshold': 0.85
    }
    
    # Stock universe
    large_caps = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'JPM', 'JNJ', 'PG', 'KO', 'WMT']
    
    print("🔍 Testing different screening strategies...")
    
    # Test conservative strategy
    print("\n📊 CONSERVATIVE VALUE STRATEGY:")
    conservative_results = scanner.screen_stocks(large_caps, conservative_criteria)
    if not conservative_results.empty:
        passed = conservative_results[conservative_results['passes_screen']]
        print(f"   Stocks passing: {len(passed)}")
        if not passed.empty:
            print("   Best candidates:")
            print(passed[['ticker', 'current_pe', 'peg_ratio']].head(3).to_string(index=False))
    
    # Test GARP strategy  
    print("\n📊 GROWTH AT REASONABLE PRICE (GARP) STRATEGY:")
    garp_results = scanner.screen_stocks(large_caps, garp_criteria)
    if not garp_results.empty:
        passed = garp_results[garp_results['passes_screen']]
        print(f"   Stocks passing: {len(passed)}")
        if not passed.empty:
            print("   Best candidates:")
            print(passed[['ticker', 'current_pe', 'peg_ratio']].head(3).to_string(index=False))
    
    return conservative_results, garp_results

def example_4_sector_analysis():
    """
    Example 4: Sector-by-sector PE analysis
    """
    print("=" * 60)
    print("EXAMPLE 4: Sector Analysis")
    print("=" * 60)
    
    scanner = PEScanner()
    
    # Define sectors
    sectors = {
        'Technology': ['AAPL', 'MSFT', 'GOOGL', 'META', 'NVDA'],
        'Healthcare': ['JNJ', 'PFE', 'UNH', 'ABBV', 'MRK'],
        'Financial': ['JPM', 'BAC', 'WFC', 'GS', 'MS'],
        'Consumer': ['AMZN', 'WMT', 'HD', 'MCD', 'NKE']
    }
    
    sector_results = {}
    
    for sector_name, tickers in sectors.items():
        print(f"\n🔍 Analyzing {sector_name} sector...")
        
        results = scanner.screen_stocks(tickers)
        
        if not results.empty:
            # Calculate sector statistics
            avg_pe = results['current_pe'].mean()
            median_pe = results['current_pe'].median()
            undervalued_count = len(results[results['potentially_undervalued']])
            
            print(f"   Average PE: {avg_pe:.2f}")
            print(f"   Median PE: {median_pe:.2f}")
            print(f"   Undervalued stocks: {undervalued_count}/{len(results)}")
            
            sector_results[sector_name] = {
                'avg_pe': avg_pe,
                'median_pe': median_pe,
                'undervalued_count': undervalued_count,
                'total_stocks': len(results),
                'data': results
            }
    
    # Sector comparison
    print("\n📊 SECTOR COMPARISON:")
    comparison_data = []
    for sector, stats in sector_results.items():
        comparison_data.append({
            'Sector': sector,
            'Avg PE': f"{stats['avg_pe']:.2f}",
            'Median PE': f"{stats['median_pe']:.2f}",
            'Undervalued': f"{stats['undervalued_count']}/{stats['total_stocks']}"
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    print(comparison_df.to_string(index=False))
    
    return sector_results

def jupyter_notebook_workflow():
    """
    Example 5: Jupyter Notebook Workflow
    This shows how to use the modular blocks in Jupyter
    """
    print("=" * 60)
    print("EXAMPLE 5: Jupyter Notebook Workflow")
    print("=" * 60)
    
    print("📝 In Jupyter, you can run these blocks separately:")
    print("=" * 40)
    
    # Block 1: Initialize
    print("\n# CELL 1: Initialize Scanner")
    print("scanner = block_1_initialize_scanner()")
    scanner = block_1_initialize_scanner()
    
    # Block 2: Single analysis
    print("\n# CELL 2: Analyze Single Stock")  
    print("pe_data = block_2_analyze_single_stock(scanner, 'AAPL')")
    pe_data = block_2_analyze_single_stock(scanner, 'AAPL')
    
    # Block 3: Multi-stock screening
    print("\n# CELL 3: Screen Multiple Stocks")
    print("results = block_3_screen_multiple_stocks(scanner)")
    results = block_3_screen_multiple_stocks(scanner)
    
    # Block 4: Custom analysis
    print("\n# CELL 4: Custom Analysis")
    print("comparison = block_4_custom_analysis(scanner)")
    comparison = block_4_custom_analysis(scanner)
    
    return scanner, pe_data, results, comparison

def example_6_visualization_demo():
    """
    Example 6: Comprehensive Visualization Demo
    Demonstrates all plotting capabilities using the integrated plotly_plots.py
    """
    print("=" * 60)
    print("EXAMPLE 6: PE Analysis Visualization Demo")
    print("=" * 60)
    
    scanner = PEScanner()
    
    # Step 1: Analyze stocks for plotting
    plot_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']
    print(f"🔍 Analyzing {len(plot_tickers)} stocks for visualization...")
    
    for ticker in plot_tickers:
        scanner.calculate_pe_metrics(ticker)
    
    # Step 2: Create PE comparison chart
    print("\n📊 1. PE Ratio Comparison Chart")
    comparison_fig = scanner.plot_pe_comparison(
        tickers=plot_tickers,
        title="Tech Giants - PE Ratio Analysis"
    )
    print("   ✅ PE comparison chart created")
    print("   💡 Use: comparison_fig.show() in Jupyter to display")
    
    # Step 3: Historical PE trend
    print("\n📈 2. Historical PE Trend Analysis")
    trend_fig = scanner.plot_historical_pe_trend('AAPL')
    if trend_fig:
        print("   ✅ Historical PE trend created for AAPL")
        print("   💡 Shows PE ratios over time with average line")
    
    # Step 4: Comprehensive dashboard
    print("\n📋 3. Valuation Dashboard")
    dashboard_fig = scanner.plot_valuation_dashboard('MSFT')
    if dashboard_fig:
        print("   ✅ Comprehensive dashboard created for MSFT")
        print("   💡 4-panel view: PE ratios, historical stats, gauge, metrics table")
    
    # Step 5: Screening with scatter plot
    print("\n🎯 4. PE vs Growth Analysis")
    screening_results = scanner.screen_stocks(
        tickers=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA'],
        criteria={'max_pe': 50, 'undervalued_threshold': 0.9}
    )
    
    if not screening_results.empty:
        scatter_fig = scanner.plot_pe_vs_growth_scatter(screening_results)
        if scatter_fig:
            print("   ✅ PE vs Growth scatter plot created")
            print("   💡 Green dots = undervalued stocks, PEG reference lines included")
    
    # Step 6: Sector analysis heatmap
    print("\n🏭 5. Sector Analysis Heatmap")
    sectors = {
        'Technology': ['AAPL', 'MSFT', 'GOOGL'],
        'E-Commerce': ['AMZN'],
        'Social Media': ['META'],
        'Semiconductors': ['NVDA']
    }
    
    sector_results = {}
    for sector_name, sector_tickers in sectors.items():
        results = scanner.screen_stocks(sector_tickers)
        if not results.empty:
            sector_results[sector_name] = {'data': results}
    
    if sector_results:
        heatmap_fig = scanner.plot_sector_pe_heatmap(sector_results)
        if heatmap_fig:
            print("   ✅ Sector PE heatmap created")
            print("   💡 Color-coded PE ratios across sectors and stocks")
    
    print("\n🎉 Visualization Demo Completed!")
    print("=" * 50)
    print("📚 JUPYTER USAGE GUIDE:")
    print("""
# In your Jupyter notebook, use these commands:

# Initialize scanner
from pe_scanner import PEScanner
scanner = PEScanner()

# Create plots
comparison_fig = scanner.plot_pe_comparison(['AAPL', 'MSFT', 'GOOGL'])
comparison_fig.show()  # Display interactive plot

dashboard = scanner.plot_valuation_dashboard('AAPL')
dashboard.show()  # 4-panel comprehensive view

trend_plot = scanner.plot_historical_pe_trend('MSFT')
trend_plot.show()  # Historical PE analysis

# Save plots
comparison_fig.write_html('pe_comparison.html')
dashboard.write_html('aapl_dashboard.html')

# Screening with visualization
results = scanner.screen_stocks(['AAPL', 'MSFT', 'GOOGL', 'AMZN'])
scatter = scanner.plot_pe_vs_growth_scatter(results)
scatter.show()  # PE vs Growth relationship
""")
    
    return {
        'comparison_chart': comparison_fig if 'comparison_fig' in locals() else None,
        'trend_analysis': trend_fig if 'trend_fig' in locals() else None,
        'dashboard': dashboard_fig if 'dashboard_fig' in locals() else None,
        'scatter_plot': scatter_fig if 'scatter_fig' in locals() else None,
        'sector_heatmap': heatmap_fig if 'heatmap_fig' in locals() else None,
        'screening_data': screening_results
    }

def run_all_examples():
    """
    Run all examples - good for testing or demonstration
    """
    print("🚀 PE SCANNER - COMPREHENSIVE EXAMPLES")
    print("=" * 70)
    
    try:
        # Example 1: Basic analysis
        scanner = example_1_basic_analysis()
        
        # Example 2: Multi-stock screening
        screening_results = example_2_multi_stock_screening()
        
        # Example 3: Custom criteria
        conservative, garp = example_3_custom_screening_criteria()
        
        # Example 4: Sector analysis
        sector_data = example_4_sector_analysis()
        
        # Example 5: Jupyter workflow
        jupyter_results = jupyter_notebook_workflow()
        
        # Example 6: Visualization demo
        visualization_data = example_6_visualization_demo()
        
        print("\n🎉 All examples completed successfully!")
        
    except Exception as e:
        print(f"❌ Error running examples: {e}")
        print("Note: Some examples may require valid API credentials")

# Jupyter Notebook Helper Functions
def quick_analysis(ticker):
    """Quick analysis function for Jupyter notebooks"""
    scanner = PEScanner()
    return scanner.calculate_pe_metrics(ticker)

def quick_screen(tickers, max_pe=25):
    """Quick screening function for Jupyter notebooks"""
    scanner = PEScanner()
    criteria = {'max_pe': max_pe, 'require_positive_eps': True}
    return scanner.screen_stocks(tickers, criteria)

def quick_plot_comparison(tickers, title="PE Comparison"):
    """Quick plotting function for PE comparison"""
    scanner = PEScanner()
    fig = scanner.plot_pe_comparison(tickers, title=title)
    return fig

def quick_plot_dashboard(ticker):
    """Quick plotting function for valuation dashboard"""
    scanner = PEScanner()
    fig = scanner.plot_valuation_dashboard(ticker)
    return fig

def quick_plot_trend(ticker):
    """Quick plotting function for historical PE trend"""
    scanner = PEScanner()
    fig = scanner.plot_historical_pe_trend(ticker)
    return fig

def quick_sector_analysis_with_plot(sectors_dict):
    """
    Quick sector analysis with heatmap visualization
    
    Args:
        sectors_dict (Dict): {'Sector Name': ['TICKER1', 'TICKER2', ...]}
        
    Returns:
        Tuple: (sector_results, heatmap_figure)
    """
    scanner = PEScanner()
    
    # Analyze all sectors
    sector_results = {}
    for sector_name, tickers in sectors_dict.items():
        results = scanner.screen_stocks(tickers)
        if not results.empty:
            sector_results[sector_name] = {
                'avg_pe': results['current_pe'].mean(),
                'median_pe': results['current_pe'].median(),
                'undervalued_count': len(results[results['potentially_undervalued']]),
                'total_stocks': len(results),
                'data': results
            }
    
    # Create heatmap
    heatmap_fig = scanner.plot_sector_pe_heatmap(sector_results)
    
    return sector_results, heatmap_fig

def jupyter_quick_start():
    """
    Print quick start guide for Jupyter notebooks
    """
    quick_start_guide = '''
# =============================================================================
# PE SCANNER - JUPYTER QUICK START GUIDE
# =============================================================================

# 1. BASIC SETUP
from pe_scanner_usage_examples import *

# 2. QUICK ANALYSIS (Single Stock)
pe_data = quick_analysis('AAPL')
dashboard = quick_plot_dashboard('AAPL')
dashboard.show()

# 3. QUICK COMPARISON (Multiple Stocks)
comparison = quick_plot_comparison(['AAPL', 'MSFT', 'GOOGL'])
comparison.show()

# 4. QUICK SCREENING
results = quick_screen(['AAPL', 'MSFT', 'GOOGL', 'AMZN'], max_pe=30)
print(results[['ticker', 'current_pe', 'potentially_undervalued']])

# 5. HISTORICAL TREND
trend = quick_plot_trend('MSFT')
trend.show()

# 6. SECTOR ANALYSIS
sectors = {
    'Technology': ['AAPL', 'MSFT', 'GOOGL'],
    'Healthcare': ['JNJ', 'PFE'],
    'Finance': ['JPM', 'BAC']
}
sector_data, heatmap = quick_sector_analysis_with_plot(sectors)
heatmap.show()

# 7. SAVE PLOTS
dashboard.write_html('dashboard.html')
comparison.write_html('comparison.html')
print("📊 Plots saved!")

# =============================================================================
# ADVANCED USAGE
# =============================================================================

# Full scanner with all capabilities
scanner = PEScanner()

# Custom screening criteria
custom_criteria = {
    'max_pe': 20,
    'min_pe': 8,
    'require_positive_eps': True,
    'max_peg_ratio': 1.5,
    'undervalued_threshold': 0.8
}

results = scanner.screen_stocks(['AAPL', 'MSFT', 'GOOGL'], custom_criteria)

# Create PE vs Growth scatter plot
scatter = scanner.plot_pe_vs_growth_scatter(results)
scatter.show()

# Generate comprehensive report
report = scanner.generate_analysis_report('AAPL')
print(report)
'''
    
    print("📚 JUPYTER QUICK START GUIDE")
    print("=" * 60)
    print("Copy these code blocks to get started quickly:")
    print(quick_start_guide)

if __name__ == "__main__":
    run_all_examples() 