# sample_data_generator.py

import pandas as pd
import numpy as np

def generate_sample_data(start='2020-01-01', end='2023-01-01'):
    # Create date range
    dates = pd.date_range(start=start, end=end, freq='B')  # Business days only
    
    # Simulate random daily price changes (using a geometric Brownian motion model)
    np.random.seed(42)
    price_changes = np.random.normal(0.0005, 0.02, len(dates))  # Daily returns with small drift
    
    # Generate prices starting at 100
    prices = 100 * np.cumprod(1 + price_changes)
    
    # Create a DataFrame with prices
    data = pd.DataFrame(data={'Price': prices}, index=dates)
    return data

# Save the sample data to CSV for testing
sample_data = generate_sample_data()
sample_data.to_csv('sample_data.csv')

# Print the head of the generated sample data
print(sample_data.head())
