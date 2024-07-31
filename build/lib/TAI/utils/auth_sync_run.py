from TAI.utils.auth_sync import AuthSync
import os

start_with = ('REDSHIFT_', 'MYSQL_', 'DYNAMODB_', 'AWS_',
               'BROKER_', 'OPENAI_', 'IB', 'ALPACA_', 
               'BLS_','FRED_','ROBINHOOD_','SLACK_')

def print_env_vars():
    print("Current environment variables:")
    for key, value in os.environ.items():
        if key.startswith(start_with):
            print(f'{key}={value}')

def main():
    # Print current environment variables before sync
    print("Before sync:")
    print_env_vars()

    # Create an instance of AuthSync
    auth_sync = AuthSync()

    # Perform the synchronization from auth.ini to environment variables
    auth_sync.sync()

    # Generate a shell script to set environment variables
    with open('set_env_vars.sh', 'w') as f:
        f.write("#!/bin/zsh\n")
        for key, value in os.environ.items():
            if key.startswith(start_with):
                f.write(f'export {key}="{value}"\n')
    
    # Generate a batch script to set environment variables for Windows
    with open('set_env_vars.bat', 'w') as f:
        for key, value in os.environ.items():
            if key.startswith(start_with):
                f.write(f'set {key}={value}\n')

    # Print current environment variables after sync
    print("\nAfter sync:")
    print_env_vars()

    print("\nRun 'source set_env_vars.sh' to set environment variables in your current Unix shell session.")
    print("Run 'set_env_vars.bat' to set environment variables in your current Windows command prompt session.")

if __name__ == "__main__":
    main()