#requires two inputs
# 1) flask_charts.json
# 2) static folder that stores charts.html files
from flask import Flask, render_template_string, abort
import json
import io

class FlaskChartServer:
    index_template = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Available Charts</title>
    </head>
    <body>
        <h1>Available Charts</h1>
        <ul>
            {% for chart_name, chart_info in charts.items() %}
            <li><a href="/plot/{{ chart_name }}">{{ chart_info['title'] if chart_info['title'] else "Untitled Chart" }}</a></li>
            {% endfor %}
        </ul>
    </body>
    </html>
    '''

    plot_html_template = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>{{ chart['title'] if chart['title'] else "" }}</title>
    </head>
    <body>
        {% if chart['title'] %}
        <h1>{{ chart['title'] }}</h1>
        {% endif %}
        <iframe src="{{ url_for('static', filename=chart['data']) }}" width="{{ width }}" height="{{ height }}"></iframe>
    </body>
    </html>
    '''

    plot_dynamic_template = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>{{ chart['title'] if chart['title'] else "" }}</title>
    </head>
    <body>
        {% if chart['title'] %}
        <h1>{{ chart['title'] }}</h1>
        {% endif %}
        <div style="width:{{ width }}; height:{{ height }};">
            {{ chart|safe }}
        </div>
    </body>
    </html>
    '''

    def __init__(self, json_config_path, plotly_charts=None, hidden_titles=None):
        """
        Initialize the FlaskChartServer with a configuration JSON file, Plotly charts dictionary, and optional hidden titles.
        
        :param json_config_path: Path to the JSON configuration file.
        :param plotly_charts: Dictionary of Plotly charts.
        :param hidden_titles: List of chart keys to hide (optional).
        """
        self.app = Flask(__name__)
        self.hidden_titles = hidden_titles or []
        self.plotly_charts = plotly_charts or {}
        self.charts = self.load_charts(json_config_path)
        self.filtered_charts = self.filter_charts(self.charts, self.hidden_titles)
        self.setup_routes()

    def load_charts(self, json_config_path):
        """Load charts from a JSON configuration file."""
        with open(json_config_path) as f:
            return json.load(f)

    def filter_charts(self, charts, hidden_titles):
        """Filter out charts based on the provided hidden titles."""
        return {key: value for key, value in charts.items() if key not in hidden_titles}

    def setup_routes(self):
        """Set up the Flask routes for the charts."""

        @self.app.route('/')
        def index():
            # Render a simple homepage listing available charts
            return render_template_string(self.index_template, charts=self.filtered_charts)

        @self.app.route('/plot/<chart_name>')
        def plot(chart_name):
            if chart_name in self.filtered_charts:
                chart_info = self.filtered_charts[chart_name]
                # Default to full iframe size if not specified
                width = chart_info.get("width", "100%")
                height = chart_info.get("height", "100%")

                if chart_info["type"] == "html":
                    # Add ".html" to the source filename
                    html_file = f"{chart_name}.html"
                    return render_template_string(self.plot_html_template, chart={"data": html_file, "title": chart_info["title"]}, width=width, height=height)

                elif chart_info["type"] == "plotly":
                    # Retrieve the Plotly figure from the dictionary
                    fig = self.plotly_charts.get(chart_name)
                    if fig:
                        buffer = io.StringIO()
                        fig.write_html(buffer)
                        buffer.seek(0)
                        return render_template_string(self.plot_dynamic_template, chart=buffer.getvalue(), width=width, height=height)
                    else:
                        abort(404)
                else:
                    abort(404)  # If the type is not supported
            else:
                abort(404)

    def run(self, debug=True):
        """Run the Flask app."""
        self.app.run(debug=True) #host='0.0.0.0', port=5000,

if __name__ == '__main__':
    import plotly.graph_objs as go
    from flask_chartserver import FlaskChartServer

    # Example Plotly figures defined in a dictionary
    plotly_charts = {
        "chart2": go.Figure(data=go.Scatter(x=[1, 2, 3], y=[4, 1, 2], mode='lines+markers'))
    }
    # plotly_charts.write_html("chart2.html")

    app_instance = FlaskChartServer(json_config_path='flask_charts.json', plotly_charts=plotly_charts, hidden_titles=[])
    app_instance.run()
