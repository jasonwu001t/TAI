#!/usr/bin/env python3
"""
Simple NativeQuant FastAPI server for testing
Now integrated within the TAI package
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uvicorn

app = FastAPI(
    title="NativeQuant API",
    description="Financial data API server",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint"""
    return HTMLResponse("""
    <html>
        <head><title>NativeQuant API</title></head>
        <body>
            <h1>🚀 NativeQuant API Server</h1>
            <p>Financial data API server is running!</p>
            <ul>
                <li><a href="/docs">📖 API Documentation</a></li>
                <li><a href="/health">🏥 Health Check</a></li>
                <li><a href="/stock_historical/AAPL">📊 Sample Stock Data</a></li>
            </ul>
        </body>
    </html>
    """)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

@app.get("/stock_historical/{symbol}")
async def get_stock_historical(symbol: str):
    """Get sample historical stock data"""
    try:
        # Generate sample data
        dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
        base_price = 100
        prices = base_price + np.cumsum(np.random.randn(30) * 2)
        
        data = []
        for i, (date, price) in enumerate(zip(dates, prices)):
            data.append({
                "timestamp": date.strftime('%Y-%m-%d'),
                "open": round(float(price * 0.999), 2),
                "high": round(float(price * 1.02), 2),
                "low": round(float(price * 0.98), 2),
                "close": round(float(price), 2),
                "volume": int(np.random.randint(1000000, 5000000)),
                "symbol": symbol.upper()
            })
        
        return {symbol.upper(): data}
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/economic/gdp")
async def get_gdp_data():
    """Get sample GDP data"""
    try:
        dates = pd.date_range(start='2020-01-01', periods=16, freq='Q')
        base_gdp = 20000
        values = [base_gdp + i * 200 + np.random.randint(-100, 100) for i in range(16)]
        
        data = [
            {"date": date.strftime('%Y-%m-%d'), "value": value}
            for date, value in zip(dates, values)
        ]
        
        return {
            "series_code": "GDP",
            "data": data,
            "info": {"title": "Gross Domestic Product", "units": "Billions of Dollars"}
        }
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Additional endpoints to match frontend expectations
@app.get("/articles")
async def get_articles():
    """Get sample articles data"""
    sample_articles = [
        {
            "id": "1",
            "title": "Market Analysis: Q4 2024 Outlook",
            "excerpt": "An in-depth analysis of market trends and predictions for the fourth quarter of 2024.",
            "category": "analysis",
            "tags": ["market", "analysis", "outlook"],
            "date": "2024-08-10",
            "imageUrl": "/placeholder.svg"
        },
        {
            "id": "2", 
            "title": "Fed Rate Decision Impact",
            "excerpt": "Understanding the implications of the latest Federal Reserve interest rate decision.",
            "category": "economy",
            "tags": ["fed", "rates", "economy"],
            "date": "2024-08-09",
            "imageUrl": "/placeholder.svg"
        }
    ]
    return sample_articles

@app.get("/articles/{article_id}")
async def get_article(article_id: str):
    """Get individual article data"""
    sample_article = {
        "id": article_id,
        "title": f"Article {article_id}",
        "content": f"This is the content for article {article_id}. Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "category": "analysis",
        "tags": ["sample", "article"],
        "date": "2024-08-10",
        "imageUrl": "/placeholder.svg"
    }
    return sample_article

@app.get("/economy")
async def get_economy_data():
    """Get economy indicators data"""
    economy_indicators = [
        {
            "name": "GDP Growth",
            "value": "2.8%",
            "change": "+0.3%",
            "trend": "up",
            "description": "Quarterly GDP growth rate"
        },
        {
            "name": "Unemployment Rate", 
            "value": "3.7%",
            "change": "-0.1%",
            "trend": "down",
            "description": "National unemployment rate"
        },
        {
            "name": "Inflation Rate",
            "value": "3.2%",
            "change": "+0.2%", 
            "trend": "up",
            "description": "Consumer Price Index inflation"
        }
    ]
    return economy_indicators

@app.get("/economy_short")
async def get_economy_short():
    """Get short economy data"""
    return [
        {"indicator": "GDP", "value": 2.8, "unit": "%"},
        {"indicator": "Unemployment", "value": 3.7, "unit": "%"},
        {"indicator": "Inflation", "value": 3.2, "unit": "%"}
    ]

@app.get("/us_treasury_yield")
async def get_treasury_yield():
    """Get US Treasury yield data"""
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    yields_2y = 4.5 + np.random.randn(30) * 0.1
    yields_10y = 4.8 + np.random.randn(30) * 0.1
    
    data = [
        {
            "date": date.strftime('%Y-%m-%d'),
            "2Y": round(float(y2), 3),
            "10Y": round(float(y10), 3)
        }
        for date, y2, y10 in zip(dates, yields_2y, yields_10y)
    ]
    
    return {"treasury_yields": data}

@app.get("/option_chain/{symbol}/{expiration}")
async def get_option_chain(symbol: str, expiration: str):
    """Get options chain data"""
    strikes = np.arange(400, 500, 5)
    calls = []
    puts = []
    
    for strike in strikes:
        calls.append({
            "strike": float(strike),
            "bid": round(float(np.random.uniform(1, 10)), 2),
            "ask": round(float(np.random.uniform(1, 10)), 2),
            "volume": int(np.random.randint(0, 1000)),
            "openInterest": int(np.random.randint(0, 5000))
        })
        puts.append({
            "strike": float(strike),
            "bid": round(float(np.random.uniform(1, 10)), 2), 
            "ask": round(float(np.random.uniform(1, 10)), 2),
            "volume": int(np.random.randint(0, 1000)),
            "openInterest": int(np.random.randint(0, 5000))
        })
    
    return {
        "symbol": symbol.upper(),
        "expiration": expiration,
        "calls": calls,
        "puts": puts
    }

if __name__ == "__main__":
    print("🚀 Starting NativeQuant API Server...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
