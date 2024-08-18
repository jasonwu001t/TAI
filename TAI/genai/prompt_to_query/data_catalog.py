import json

class CentralDataCatalog:
    def __init__(self):
        self.catalog = {}

    def load_from_json(self, json_file):
        """
        Load the catalog from a JSON file.

        Args:
        json_file (str): Path to the JSON file.

        Returns:
        None
        """
        # from TAI.data import DataMaster
        # dm = DataMaster()
        # cur_path = dm.get_current_dir()
        # json_file_path = os.path.join(cur_path, 'data_catalog.json')

        with open(json_file, 'r') as f:
            data = json.load(f)
            for table_name, info in data.items():
                self.add_table(
                    table_name,
                    info.get('description', ''),
                    info.get('embedding', None)
                )

    def add_table(self, table_name, description, embedding=None):
        self.catalog[table_name] = {
            'description': description,
            'embedding': embedding
        }

    def get_catalog_info(self, table_name):
        return self.catalog.get(table_name, None)

    def list_tables(self):
        return list(self.catalog.keys())
