# app/flask_app.py
from flask import Flask, jsonify, render_template_string
import pandas as pd
from TAI.data.fred import Fred
from TAI.analytics.plotly_plots import QuickPlot

class FlaskApp:
    def __init__(self):
        self.app = Flask(__name__)
        self.fred = Fred()

    def setup_routes(self):
        @self.app.route('/data/<series_code>')
        def get_data(series_code):
            data = self.fred.get_series(series_code).reset_index()
            data.columns = ['date', 'value']
            return data.to_json(orient='records')

        @self.app.route('/plot/<series_code>')
        def plot_data(series_code):
            data = self.fred.get_series(series_code).reset_index()
            data.columns = ['date', 'value']
            qp = QuickPlot(dataframes=[data], labels=[series_code])
            fig = qp.plot_line(title=f"Plot of {series_code}")
            graph_html = fig.to_html(full_html=False)
            return render_template_string(f"<html><body>{graph_html}</body></html>")

    def run(self, host='0.0.0.0', port=5000):
        self.setup_routes()
        self.app.run(host=host, port=port)

if __name__ == "__main__":
    app = FlaskApp()
    app.run()
