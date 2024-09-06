import numpy as np
import pandas as pd
from scipy.stats import norm

def black_scholes(S, K, T, r, sigma, option_type="call"):
    """Black-Scholes formula to calculate option price and time value."""
    if T <= 0:  # Ensure time doesn't go to zero to avoid division by zero
        return max(0, S - K) if option_type == "call" else max(0, K - S)
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    if option_type == "call":
        option_price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    elif option_type == "put":
        option_price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    
    return option_price

def option_profit_table(symbol, 
                        current_price, 
                        option_type, 
                        position, 
                        date_range, 
                        strike_price, 
                        premium, 
                        contract_size=100, 
                        price_step=1, 
                        r=0.01,  # interest rate
                        sigma=0.3):  # implied volatility
    """Calculate the profit/loss for an options strategy over a range of stock prices and expiration dates."""
    
    stock_price_range = np.arange(int(current_price * 0.95), int(current_price * 1.05) + 1, price_step)
    profit_matrix = []

    date_range = pd.date_range(start=date_range[0], end=date_range[1])
    expiration_date = date_range[-1]
    time_to_expiration = (expiration_date - date_range).days / 365.0

    for stock_price in stock_price_range:
        daily_profits = []
        for T in time_to_expiration:
            time_value = black_scholes(stock_price, strike_price, T, r, sigma, option_type)

            if option_type == 'call':
                if position == 'buy':
                    value_at_expiry = max(stock_price - strike_price, 0)
                    profit = (value_at_expiry - premium) * contract_size if T == 0 else (value_at_expiry - premium + time_value) * contract_size
                elif position == 'write':
                    value_at_expiry = max(stock_price - strike_price, 0)
                    profit = (premium * contract_size) - (value_at_expiry * contract_size) if T == 0 else (premium * contract_size) - (value_at_expiry + time_value) * contract_size

            elif option_type == 'put':
                if position == 'buy':
                    value_at_expiry = max(strike_price - stock_price, 0)
                    profit = (value_at_expiry - premium) * contract_size if T == 0 else (value_at_expiry - premium + time_value) * contract_size
                elif position == 'write':
                    max_loss = (strike_price * contract_size) - (premium * contract_size)
                    value_at_expiry = max(strike_price - stock_price, 0)
                    profit = (premium * contract_size) - (value_at_expiry * contract_size) if T == 0 else (premium * contract_size) - (value_at_expiry + time_value) * contract_size
                    profit = max(profit, -max_loss)

            daily_profits.append(profit)
        profit_matrix.append(daily_profits)

    profit_df = pd.DataFrame(profit_matrix, columns=[f"{date.date()}" for date in date_range], index=[f"${price:.2f}" for price in stock_price_range])

    if option_type == 'call':
        breakeven_price = strike_price + premium
    elif option_type == 'put':
        breakeven_price = strike_price - premium

    summary = {
        'Symbol': symbol,
        'Current Price': current_price,
        'Strike Price': strike_price,
        'Premium Paid' if position == 'buy' else 'Premium Received': premium,
        'Breakeven Price': breakeven_price,
    }

    if option_type == 'call':
        summary.update({
            'Max Profit': 'infinite' if position == 'buy' else 'Limited to premium received',
            'Max Risk': premium * contract_size if position == 'buy' else 'Unlimited Risk',
        })
    elif option_type == 'put':
        if position == 'buy':
            summary.update({
                'Max Profit': f'Limited to {strike_price - premium} per contract',
                'Max Risk': premium * contract_size,
            })
        elif position == 'write':
            max_risk = (strike_price * contract_size) - (premium * contract_size)
            summary.update({
                'Max Profit': premium * contract_size,  # Premium received
                'Max Risk': max_risk,  # Risk if stock drops to 0
            })

    summary['Breakeven at Expiry'] = breakeven_price

    return profit_df, summary
