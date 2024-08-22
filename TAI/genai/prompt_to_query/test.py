from text_to_sql_agent import TextToSQLAgent

agent = TextToSQLAgent()
user_prompt = "Wwhat "
result = agent.process_prompt(user_prompt)
print(result)
