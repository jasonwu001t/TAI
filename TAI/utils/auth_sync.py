import os
import configparser

class AuthSync:
    def __init__(self, config_file='auth.ini'):
        self.config_file = config_file
        self.config = configparser.ConfigParser()

    def read_config(self):
        if os.path.exists(self.config_file):
            self.config.read(self.config_file)
        else:
            raise FileNotFoundError(f"The configuration file {self.config_file} does not exist.")

    def clear_env_vars(self):
        for section in self.config.sections():
            for key in self.config.options(section):
                env_var = f"{section.upper()}_{key.upper()}"
                if env_var in os.environ:
                    print(f"Deleting existing environment variable {env_var}")
                    del os.environ[env_var]

    def set_env_vars_from_config(self):
        for section in self.config.sections():
            for key, value in self.config.items(section):
                env_var = f"{section.upper()}_{key.upper()}"
                os.environ[env_var] = value
                print(f"Set environment variable {env_var} = {value}")

    def show_env_vars(self):
        print("Current environment variables:")
        for key, value in os.environ.items():
            if key.startswith(('REDSHIFT_', 'MYSQL_', 'DYNAMODB_', 'AWS_',
               'BROKER_', 'OPENAI_', 'IB', 'ALPACA_', 'BLS_')):
                print(f'{key}={value}')

    def sync(self):
        print("Before sync:")
        self.show_env_vars()
        self.read_config()
        self.clear_env_vars()
        self.set_env_vars_from_config()
        print("\nAfter sync:")
        self.show_env_vars()
