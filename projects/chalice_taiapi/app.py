# chalice deploy, chalice delete

from chalice import Chalice, Response
import boto3
import json
from botocore.exceptions import ClientError

app = Chalice(app_name='taiapi')

# Initialize boto3 client for S3
s3_client = boto3.client('s3')

BUCKET_NAME = 'jtrade1-dir'  # S3 bucket name

# Helper function to fetch JSON data from S3


def fetch_json_from_s3(key: str):
    """Fetch JSON data from S3 bucket."""
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except ClientError as e:
        return {"error": f"Unable to fetch {key} from S3: {str(e)}"}
    except json.JSONDecodeError:
        return {"error": f"Error decoding JSON in {key}"}

# Helper function to create JSON responses


def create_json_response(body, status_code=200):
    return Response(
        body=body,
        status_code=status_code,
        headers={'Content-Type': 'application/json'}
    )


@app.route('/us_treasury_yield', methods=['GET'], cors=True)
def get_categories():
    categories = fetch_json_from_s3('api/treasury_yield_all.json')
    if "error" in categories:
        return create_json_response({'message': 'Internal server error', 'details': categories["error"]}, status_code=500)
    return create_json_response(categories)

# Endpoint to get all articles or filter by id


@app.route('/articles', methods=['GET'], cors=True)
@app.route('/articles/{id}', methods=['GET'], cors=True)
def get_articles(id=None):
    articles = fetch_json_from_s3('articles.json')
    if "error" in articles:
        return create_json_response({'message': 'Internal server error', 'details': articles["error"]}, status_code=500)

    if id:
        # Filter articles by id
        article = next((a for a in articles if a['id'] == id), None)
        if article:
            return create_json_response(article)
        else:
            return create_json_response({'message': f'Article with id {id} not found'}, status_code=404)

    return create_json_response(articles)

# Endpoint to get all chart data or filter by id


@app.route('/chart', methods=['GET'], cors=True)
@app.route('/chart/{id}', methods=['GET'], cors=True)
def get_chart_data(id=None):
    chart_data = fetch_json_from_s3('chart_data.json')
    if "error" in chart_data:
        return create_json_response({'message': 'Internal server error', 'details': chart_data["error"]}, status_code=500)

    if id:
        # Filter chart data by id
        chart = next((c for c in chart_data if c['id'] == id), None)
        if chart:
            return create_json_response(chart)
        else:
            return create_json_response({'message': f'Chart with id {id} not found'}, status_code=404)

    return create_json_response(chart_data)

# Endpoint to get all categories from S3


@app.route('/categories', methods=['GET'], cors=True)
def get_categories():
    categories = fetch_json_from_s3('categories.json')
    if "error" in categories:
        return create_json_response({'message': 'Internal server error', 'details': categories["error"]}, status_code=500)
    return create_json_response(categories)

# Endpoint to get all indicators from S3


@app.route('/indicators', methods=['GET'], cors=True)
def get_indicators():
    indicators = fetch_json_from_s3('indicators.json')
    if "error" in indicators:
        return create_json_response({'message': 'Internal server error', 'details': indicators["error"]}, status_code=500)
    return create_json_response(indicators)
