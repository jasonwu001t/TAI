
from TAI.strategy import OptionBet
# from GTI.broker import IBTrade


# ticker = 'TSLA'
# ib_handler = IBTrade()

# # Get stock quote for TSLA
# quote = ib_handler.get_stock_quote(ticker)
# print("Stock Quote:", quote)

ticker = 'COIN'
chose_expiry_date = '2024-08-16'
option_bet = OptionBet(ticker, chose_expiry_date)
describe_df = option_bet.describe_perc_change()
print (describe_df)