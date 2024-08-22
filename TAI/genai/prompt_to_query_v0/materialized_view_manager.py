import polars as pl

class MaterializedViewManager:
    def __init__(self):
        self.views = {}

    def create_view(self, name, query, data):
        result = data.lazy().sql(query).collect()
        self.views[name] = result

    def refresh_view(self, name, query, data):
        result = data.lazy().sql(query).collect()
        self.views[name] = result

    def get_view(self, name):
        return self.views.get(name, None)

    def view_exists(self, name):
        return name in self.views
