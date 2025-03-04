import logging
from logging.handlers import RotatingFileHandler

def setup_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 文件日志（自动轮转）
    file_handler = RotatingFileHandler(
        'app.log', maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    
    # 控制台日志
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger