"""
NativeQuant FastAPI Application
A comprehensive API server leveraging the TAI package for financial data, analytics, and AI capabilities.

This FastAPI application provides unified access to:
- Financial data sources (Alpaca, Robinhood, Interactive Brokers, Polygon)
- Economic data (FRED, BLS, Treasury, FOMC)
- SEC filings and fundamental analysis
- Options trading and analytics
- GenAI capabilities (AWS Bedrock, Text-to-SQL)
- Financial analytics and visualizations
"""

from fastapi import FastAPI, HTTPException, Query, Body, Path
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date, timedelta
from pydantic import BaseModel, Field
import pandas as pd
import json
import logging
import uvicorn

# TAI Package Imports (now we're inside the TAI package)
import sys
import os

# Add the parent directory to the path to import TAI modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from TAI.source import (
        Alpaca, Fred, BLS, SEC, Treasury, FOMC, Robinhood, Atom
    )
    from TAI.analytics import DataAnalytics, QuickPlot, QuantStatsPlot
    from TAI.genai import AWSBedrock, TextToSQLAgent, APIAgent
    TAI_AVAILABLE = True
except ImportError as e:
    logger.warning(f"TAI package not available: {e}")
    TAI_AVAILABLE = False
    # Create mock classes for development
    class MockService:
        def __init__(self):
            pass
    
    Alpaca = Fred = BLS = SEC = Treasury = FOMC = Robinhood = Atom = MockService
    DataAnalytics = QuickPlot = QuantStatsPlot = AWSBedrock = TextToSQLAgent = APIAgent = MockService

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic Models
class StockDataRequest(BaseModel):
    symbol: str = Field(..., description="Stock symbol (e.g., AAPL)")
    lookback_period: Optional[str] = Field("30D", description="Lookback period (e.g., 30D, 1Y)")
    timeframe: Optional[str] = Field("Day", description="Timeframe (Min, Hour, Day, Week, Month)")

class OptionChainRequest(BaseModel):
    underlying_symbol: str = Field(..., description="Underlying stock symbol")
    strike_price_gte: Optional[float] = Field(None, description="Minimum strike price")
    strike_price_lte: Optional[float] = Field(None, description="Maximum strike price")
    expiration_date: Optional[str] = Field(None, description="Expiration date (YYYY-MM-DD)")
    type: Optional[str] = Field(None, description="Option type (call or put)")

class EconomicDataRequest(BaseModel):
    series_code: str = Field(..., description="Economic series code")
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")

class SECFilingRequest(BaseModel):
    symbol: str = Field(..., description="Company ticker symbol")
    metrics: Optional[List[str]] = Field(None, description="List of financial metrics to retrieve")
    lookback_years: Optional[int] = Field(5, description="Number of years to look back")

class AnalyticsRequest(BaseModel):
    data_type: str = Field(..., description="Type of analysis (correlation, volatility, etc.)")
    symbols: List[str] = Field(..., description="List of symbols to analyze")
    period: Optional[str] = Field("1Y", description="Analysis period")

class ChatRequest(BaseModel):
    message: str = Field(..., description="User message/query")
    context: Optional[str] = Field(None, description="Additional context")

# Initialize FastAPI app
app = FastAPI(
    title="NativeQuant API",
    description="Comprehensive financial data and analytics API powered by TAI",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services (with error handling)
services = {}

def init_services():
    """Initialize all TAI services with error handling"""
    if not TAI_AVAILABLE:
        logger.warning("TAI package not available - running in demo mode")
        return
    
    try:
        services['alpaca'] = Alpaca()
        logger.info("✅ Alpaca service initialized")
    except Exception as e:
        logger.warning(f"❌ Alpaca service failed: {e}")
    
    try:
        services['fred'] = Fred()
        logger.info("✅ FRED service initialized")
    except Exception as e:
        logger.warning(f"❌ FRED service failed: {e}")
    
    try:
        services['bls'] = BLS()
        logger.info("✅ BLS service initialized")
    except Exception as e:
        logger.warning(f"❌ BLS service failed: {e}")
    
    try:
        services['sec'] = SEC()
        logger.info("✅ SEC service initialized")
    except Exception as e:
        logger.warning(f"❌ SEC service failed: {e}")
    
    try:
        services['treasury'] = Treasury()
        logger.info("✅ Treasury service initialized")
    except Exception as e:
        logger.warning(f"❌ Treasury service failed: {e}")
    
    try:
        services['fomc'] = FOMC()
        logger.info("✅ FOMC service initialized")
    except Exception as e:
        logger.warning(f"❌ FOMC service failed: {e}")
    
    try:
        services['robinhood'] = Robinhood()
        logger.info("✅ Robinhood service initialized")
    except Exception as e:
        logger.warning(f"❌ Robinhood service failed: {e}")
    
    try:
        services['atom'] = Atom()
        logger.info("✅ Atom service initialized")
    except Exception as e:
        logger.warning(f"❌ Atom service failed: {e}")
    
    try:
        services['bedrock'] = AWSBedrock()
        logger.info("✅ AWS Bedrock service initialized")
    except Exception as e:
        logger.warning(f"❌ AWS Bedrock service failed: {e}")
    
    # Analytics services (these should always work if TAI is available)
    try:
        services['analytics'] = DataAnalytics()
        services['quick_plot'] = QuickPlot()
        services['quant_plot'] = QuantStatsPlot()
        logger.info("✅ Analytics services initialized")
    except Exception as e:
        logger.warning(f"❌ Analytics services failed: {e}")

# Health Check Endpoints
@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with API overview"""
    html_content = """
    <html>
        <head>
            <title>NativeQuant API</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .service { padding: 10px; margin: 5px 0; border-radius: 5px; }
                .available { background-color: #d4edda; color: #155724; }
                .unavailable { background-color: #f8d7da; color: #721c24; }
            </style>
        </head>
        <body>
            <h1>🚀 NativeQuant API Server</h1>
            <p>Comprehensive financial data and analytics API powered by TAI</p>
            
            <h2>📊 Available Services</h2>
            """ + "".join([
                f'<div class="service {"available" if service in services else "unavailable"}">'
                f'{"✅" if service in services else "❌"} {service.upper()}</div>'
                for service in ['alpaca', 'fred', 'bls', 'sec', 'treasury', 'fomc', 'robinhood', 'atom', 'bedrock', 'analytics']
            ]) + """
            
            <h2>🔗 Quick Links</h2>
            <ul>
                <li><a href="/docs">📖 API Documentation (Swagger UI)</a></li>
                <li><a href="/redoc">📚 API Documentation (ReDoc)</a></li>
                <li><a href="/health">🏥 Health Check</a></li>
                <li><a href="/services">🔧 Service Status</a></li>
            </ul>
            
            <h2>🎯 Sample Endpoints</h2>
            <ul>
                <li><strong>Stock Data:</strong> <code>GET /stock/historical/AAPL</code></li>
                <li><strong>Economic Data:</strong> <code>GET /economic/fred/gdp</code></li>
                <li><strong>Options Chain:</strong> <code>GET /options/chain/SPY</code></li>
                <li><strong>SEC Filings:</strong> <code>GET /sec/financials/AAPL</code></li>
                <li><strong>Analytics:</strong> <code>POST /analytics/correlation</code></li>
                <li><strong>Chat AI:</strong> <code>POST /ai/chat</code></li>
            </ul>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services_available": len(services),
        "version": "1.0.0"
    }

@app.get("/services")
async def service_status():
    """Get status of all services"""
    status = {}
    for service_name in ['alpaca', 'fred', 'bls', 'sec', 'treasury', 'fomc', 'robinhood', 'atom', 'bedrock']:
        status[service_name] = service_name in services
    
    return {
        "services": status,
        "total_available": sum(status.values()),
        "total_services": len(status)
    }

# Stock Data Endpoints
@app.get("/stock/historical/{symbol}")
async def get_stock_historical(
    symbol: str = Path(..., description="Stock symbol"),
    lookback_period: str = Query("30D", description="Lookback period"),
    timeframe: str = Query("Day", description="Timeframe")
):
    """Get historical stock data"""
    if 'alpaca' not in services:
        raise HTTPException(status_code=503, detail="Alpaca service not available")
    
    try:
        data = services['alpaca'].get_stock_historical(
            symbol.upper(), 
            lookback_period=lookback_period,
            timeframe=timeframe
        )
        
        # Convert to JSON-serializable format
        if hasattr(data, 'to_dict'):
            result = data.to_dict(orient='records')
        else:
            result = str(data)
            
        return {symbol.upper(): result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/stock/quote/{symbol}")
async def get_stock_quote(symbol: str = Path(..., description="Stock symbol")):
    """Get latest stock quote"""
    if 'alpaca' not in services:
        raise HTTPException(status_code=503, detail="Alpaca service not available")
    
    try:
        data = services['alpaca'].get_latest_quote(symbol.upper())
        return {symbol.upper(): data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Options Data Endpoints
@app.get("/options/chain/{symbol}")
async def get_options_chain(
    symbol: str = Path(..., description="Underlying symbol"),
    strike_min: Optional[float] = Query(None, description="Minimum strike price"),
    strike_max: Optional[float] = Query(None, description="Maximum strike price"),
    expiration_date: Optional[str] = Query(None, description="Expiration date (YYYY-MM-DD)")
):
    """Get options chain for a symbol"""
    if 'alpaca' not in services:
        raise HTTPException(status_code=503, detail="Alpaca service not available")
    
    try:
        data = services['alpaca'].get_option_chain(
            underlying_symbol=symbol.upper(),
            strike_price_gte=strike_min,
            strike_price_lte=strike_max,
            expiration_date=expiration_date,
            expiration_date_gte=None,
            expiration_date_lte=None
        )
        
        if hasattr(data, 'to_dict'):
            result = data.to_dict(orient='records')
        else:
            result = str(data)
            
        return {symbol.upper(): result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Economic Data Endpoints
@app.get("/economic/fred/{series_code}")
async def get_fred_data(
    series_code: str = Path(..., description="FRED series code or alias"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Get FRED economic data"""
    if 'fred' not in services:
        raise HTTPException(status_code=503, detail="FRED service not available")
    
    try:
        data = services['fred'].get_series(series_code)
        
        # Apply date filtering if provided
        if start_date:
            data = data[data.index >= pd.to_datetime(start_date)]
        if end_date:
            data = data[data.index <= pd.to_datetime(end_date)]
        
        # Convert to JSON format
        result = data.reset_index()
        result.columns = ['date', 'value']
        result['date'] = result['date'].dt.strftime('%Y-%m-%d')
        
        return {
            "series_code": series_code,
            "data": result.to_dict(orient='records'),
            "info": services['fred'].get_info(series_code) if hasattr(services['fred'], 'get_info') else None
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/economic/fred/series")
async def get_fred_series_list():
    """Get list of available FRED series"""
    if 'fred' not in services:
        raise HTTPException(status_code=503, detail="FRED service not available")
    
    try:
        if hasattr(services['fred'], 'series_codes'):
            return {"available_series": services['fred'].series_codes}
        else:
            return {"message": "Series codes not available in current implementation"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/economic/bls/{series_id}")
async def get_bls_data(series_id: str = Path(..., description="BLS series ID")):
    """Get BLS economic data"""
    if 'bls' not in services:
        raise HTTPException(status_code=503, detail="BLS service not available")
    
    try:
        data = services['bls'].fetch_bls_data([series_id])
        return {series_id: data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/economic/treasury/rates")
async def get_treasury_rates():
    """Get Treasury interest rates"""
    if 'treasury' not in services:
        raise HTTPException(status_code=503, detail="Treasury service not available")
    
    try:
        data = services['treasury'].get_rates()
        return {"treasury_rates": data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# SEC Data Endpoints
@app.get("/sec/financials/{symbol}")
async def get_sec_financials(
    symbol: str = Path(..., description="Company ticker symbol"),
    metrics: Optional[str] = Query(None, description="Comma-separated list of metrics"),
    years: int = Query(5, description="Number of years to retrieve")
):
    """Get SEC financial data for a company"""
    if 'sec' not in services:
        raise HTTPException(status_code=503, detail="SEC service not available")
    
    try:
        metrics_list = metrics.split(',') if metrics else None
        data = services['sec'].get_financial_data(
            symbol.upper(),
            metrics_to_retrieve=metrics_list,
            lookback_years=years
        )
        return {symbol.upper(): data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/sec/available-metrics")
async def get_sec_available_metrics():
    """Get list of available SEC financial metrics"""
    if 'sec' not in services:
        raise HTTPException(status_code=503, detail="SEC service not available")
    
    try:
        if hasattr(services['sec'], 'get_available_metrics'):
            metrics = services['sec'].get_available_metrics()
            return {"available_metrics": metrics}
        else:
            return {"message": "Available metrics method not found"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Analytics Endpoints
@app.post("/analytics/correlation")
async def calculate_correlation(request: AnalyticsRequest):
    """Calculate correlation between multiple securities"""
    try:
        # This would need implementation based on the specific analytics requirements
        analytics = services['analytics']
        
        # Mock response for now - would need actual implementation
        return {
            "analysis_type": "correlation",
            "symbols": request.symbols,
            "period": request.period,
            "message": "Analytics endpoint ready - implementation needed based on specific requirements"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/analytics/volatility")
async def calculate_volatility(request: AnalyticsRequest):
    """Calculate volatility analysis"""
    try:
        analytics = services['analytics']
        
        # Mock response - would need actual implementation
        return {
            "analysis_type": "volatility",
            "symbols": request.symbols,
            "period": request.period,
            "message": "Volatility analysis endpoint ready"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# AI/Chat Endpoints
@app.post("/ai/chat")
async def chat_with_ai(request: ChatRequest):
    """Chat with AI using AWS Bedrock"""
    if 'bedrock' not in services:
        raise HTTPException(status_code=503, detail="AWS Bedrock service not available")
    
    try:
        response = services['bedrock'].conversation(prompt=request.message)
        return {
            "user_message": request.message,
            "ai_response": response,
            "context": request.context,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/ai/text-to-sql")
async def text_to_sql(request: ChatRequest):
    """Convert natural language to SQL query"""
    try:
        # This would need proper setup with data catalogs
        return {
            "user_query": request.message,
            "sql_query": "-- SQL generation requires data catalog setup",
            "message": "Text-to-SQL endpoint ready - requires data catalog configuration"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Visualization Endpoints
@app.get("/plot/fred/{series_code}")
async def plot_fred_data(series_code: str = Path(..., description="FRED series code")):
    """Generate plot for FRED economic data"""
    if 'fred' not in services:
        raise HTTPException(status_code=503, detail="FRED service not available")
    
    try:
        data = services['fred'].get_series(series_code)
        df = data.reset_index()
        df.columns = ['date', 'value']
        
        quick_plot = services['quick_plot']
        fig = quick_plot.plot_line([df], labels=[series_code], title=f"FRED Series: {series_code}")
        
        # Return HTML plot
        graph_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
        return HTMLResponse(content=f"<html><body>{graph_html}</body></html>")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Demo Data Endpoints (for testing without TAI dependencies)
@app.get("/demo/stock_historical/{symbol}")
async def demo_stock_historical(symbol: str = Path(..., description="Stock symbol")):
    """Demo endpoint with sample stock data"""
    import numpy as np
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    prices = 100 + np.cumsum(np.random.randn(30) * 0.02)
    
    demo_data = [
        {
            "timestamp": date.strftime('%Y-%m-%d'),
            "open": float(price * 0.999),
            "high": float(price * 1.01),
            "low": float(price * 0.99),
            "close": float(price),
            "volume": int(np.random.randint(1000000, 5000000)),
            "symbol": symbol.upper()
        }
        for date, price in zip(dates, prices)
    ]
    
    return {symbol.upper(): demo_data}

@app.get("/demo/economic/gdp")
async def demo_economic_data():
    """Demo endpoint with sample economic data"""
    dates = pd.date_range(start='2020-01-01', periods=20, freq='Q')
    values = [20000 + i * 100 + np.random.randint(-50, 50) for i in range(20)]
    
    demo_data = [
        {"date": date.strftime('%Y-%m-%d'), "value": value}
        for date, value in zip(dates, values)
    ]
    
    return {
        "series_code": "GDP",
        "data": demo_data,
        "info": {"title": "Gross Domestic Product", "units": "Billions of Dollars"}
    }

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("🚀 Starting NativeQuant API Server...")
    init_services()
    logger.info(f"✅ Server started with {len(services)} services available")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
