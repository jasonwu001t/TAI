from option_profit_calculator import option_profit_table
from option_profit_plotter import plot_profit_loss_table

# Example usage
symbol = "SPY"
current_price = 538.91
option_type = "put"  # can be "call" or "put"
position = "write"  # can be "buy" or "write"
date_range = ["2024-09-06", "2024-09-19"]  # Date range including expiration date
strike_price = 520
premium = 2.50  # Premium paid, aka. price per option
contract_size = 100

# Generate the profit/loss table
profit_df, summary = option_profit_table(symbol, current_price, option_type, position, date_range, strike_price, premium, contract_size)

# Print the summary
print("Summary Section:")
for k, v in summary.items():
    print(f"{k}: {v}")

# Plot the profit/loss table
plot_profit_loss_table(profit_df, symbol, option_type)
