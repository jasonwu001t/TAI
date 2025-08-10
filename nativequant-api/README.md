# NativeQuant API Server

A comprehensive FastAPI application that leverages the TAI (Trading Analytics Intelligence) package to provide unified access to financial data, analytics, and AI capabilities.

## Features

### 📊 Data Sources
- **Stock Data**: Alpaca Markets integration for historical and real-time stock data
- **Options Data**: Complete options chains and Greeks
- **Economic Data**: FRED, BLS, Treasury, and FOMC data
- **SEC Filings**: Company fundamentals and financial metrics
- **Alternative Data**: Robinhood sentiment and market data

### 🧠 AI Capabilities
- **AWS Bedrock**: Conversational AI for financial analysis
- **Text-to-SQL**: Natural language to SQL query conversion
- **Embeddings**: Semantic search and similarity analysis

### 📈 Analytics
- **Financial Analytics**: Correlation, volatility, technical indicators
- **Visualization**: Interactive Plotly charts and reports
- **Portfolio Analytics**: Performance metrics and risk analysis

## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd nativequant-api

# Install dependencies
pip install -r requirements.txt

# Install TAI package
cd ../TAI
pip install -e .
```

### Configuration

Set up your API credentials as environment variables or in the TAI auth.ini file:

```bash
# Trading APIs
export ALPACA_API_KEY="your_alpaca_key"
export ALPACA_API_SECRET="your_alpaca_secret"
export ALPACA_BASE_URL="https://paper-api.alpaca.markets"

# Economic Data
export FRED_API_KEY="your_fred_key"
export BLS_API_KEY="your_bls_key"

# AI Services
export AWS_ACCESS_KEY_ID="your_aws_key"
export AWS_SECRET_ACCESS_KEY="your_aws_secret"
export AWS_REGION_NAME="us-east-1"

# SEC Data
export SEC_USER_AGENT="YourApp/1.0 (your-email@example.com)"
```

### Running the Server

```bash
# Development mode with auto-reload
python main.py

# Or using uvicorn directly
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at:
- **API**: http://localhost:8000
- **Documentation**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## API Endpoints

### Stock Data
- `GET /stock/historical/{symbol}` - Historical stock data
- `GET /stock/quote/{symbol}` - Latest stock quote

### Options Data
- `GET /options/chain/{symbol}` - Options chain data

### Economic Data
- `GET /economic/fred/{series_code}` - FRED economic data
- `GET /economic/bls/{series_id}` - BLS economic data
- `GET /economic/treasury/rates` - Treasury rates

### SEC Data
- `GET /sec/financials/{symbol}` - Company financial data
- `GET /sec/available-metrics` - Available financial metrics

### Analytics
- `POST /analytics/correlation` - Correlation analysis
- `POST /analytics/volatility` - Volatility analysis

### AI Services
- `POST /ai/chat` - Chat with AI
- `POST /ai/text-to-sql` - Natural language to SQL

### Visualization
- `GET /plot/fred/{series_code}` - Interactive plots

### System
- `GET /` - API overview and status
- `GET /health` - Health check
- `GET /services` - Service availability status

## Example Usage

### Get Stock Data
```bash
curl "http://localhost:8000/stock/historical/AAPL?lookback_period=30D&timeframe=Day"
```

### Get Economic Data
```bash
curl "http://localhost:8000/economic/fred/gdp"
```

### Chat with AI
```bash
curl -X POST "http://localhost:8000/ai/chat" \
  -H "Content-Type: application/json" \
  -d '{"message": "What is the current state of the economy?"}'
```

### Get Options Chain
```bash
curl "http://localhost:8000/options/chain/SPY?strike_min=400&strike_max=450"
```

## Architecture

The API is built with:
- **FastAPI**: Modern, fast web framework for APIs
- **TAI Package**: Comprehensive financial data and analytics library
- **Pydantic**: Data validation and serialization
- **Uvicorn**: ASGI server implementation

## Error Handling

The API includes comprehensive error handling:
- Service availability checks
- Graceful degradation when services are unavailable
- Detailed error messages and status codes
- Health check endpoints for monitoring

## Development

### Adding New Endpoints

1. Define Pydantic models for request/response
2. Add endpoint function with proper documentation
3. Include error handling and validation
4. Test the endpoint

### Service Management

Services are initialized on startup with graceful error handling. If a service fails to initialize, it will be marked as unavailable, and endpoints using that service will return a 503 status code.

## Deployment

### Docker Deployment

```dockerfile
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
COPY ../TAI ./TAI
RUN cd TAI && pip install -e .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Production Considerations

- Set up proper environment variables for API keys
- Use a production ASGI server like Gunicorn with Uvicorn workers
- Implement rate limiting and authentication as needed
- Set up monitoring and logging
- Configure CORS for your frontend domain

## License

This project uses the same license as the TAI package (MIT License).
