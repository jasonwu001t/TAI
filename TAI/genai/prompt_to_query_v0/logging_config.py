import logging

def init_logger():
    logger = logging.getLogger('TextToSQL')
    handler = logging.FileHandler('text_to_sql.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
