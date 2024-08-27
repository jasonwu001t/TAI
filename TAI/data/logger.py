import logging

def init_logger(logger_name='log', log_file_name='log_record.log'):
    logger = logging.getLogger(logger_name)
    
    # Check if the logger already has handlers to avoid adding multiple handlers
    if not logger.hasHandlers():
        handler = logging.FileHandler(log_file_name)
        console_handler = logging.StreamHandler()  # Also log to the console
 
        formatter = logging.Formatter('%(asctime)s- %(message)s') # - %(name)s - %(levelname)s 
        handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.addHandler(console_handler)
        logger.setLevel(logging.INFO)
    
    return logger

# Example use
# logger.info(f"Attempting to execute SQL query: {sql_query}")
# logger.error("Max retries reached. Failed to generate a valid SQL query.")