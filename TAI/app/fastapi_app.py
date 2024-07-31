# app/fastapi_app.py
from fastapi import FastAPI
import pandas as pd
from TAI.data.fred import Fred
from TAI.analytics.plotly_plots import QuickPlot
from fastapi.responses import JSONResponse, HTMLResponse

class FastAPIApp:
    def __init__(self):
        self.app = FastAPI()
        self.fred = Fred()

        @self.app.get("/data/{series_code}")
        async def get_data(series_code: str):
            data = self.fred.get_series(series_code).reset_index()
            data.columns = ['date', 'value']
            return JSONResponse(content=data.to_dict(orient='records'))

        @self.app.get("/plot/{series_code}")
        async def plot_data(series_code: str):
            data = self.fred.get_series(series_code).reset_index()
            data.columns = ['date', 'value']
            qp = QuickPlot(dataframes=[data], labels=[series_code])
            fig = qp.plot_line(title=f"Plot of {series_code}")
            graph_html = fig.to_html(full_html=False)
            return HTMLResponse(content=f"<html><body>{graph_html}</body></html>")

if __name__ == "__main__":
    app = FastAPIApp()
    import uvicorn
    uvicorn.run(app.app, host="0.0.0.0", port=8000)
