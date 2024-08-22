from text_to_sql_agent import TextToSQLAgent

agent = TextToSQLAgent()
user_prompt = "join the orders to the customer table then tell me how much sells"
result = agent.process_prompt(user_prompt)
print(result)
