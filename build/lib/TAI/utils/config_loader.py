import configparser,os

class ConfigLoader:
    def __init__(self, config_file='auth.ini'):
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self.config.read(self.get_config_path())

    def get_config_path(self):
        package_dir = os.path.dirname(__file__)
        return os.path.join(package_dir, self.config_file)

    def get_config(self, section, key):
        return os.getenv(f"{section.upper()}_{key.upper()}") or self.config.get(section, key, fallback=None)

# Test testkkkdsdf