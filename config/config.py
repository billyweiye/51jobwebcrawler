from configparser import ConfigParser
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

def load_config():
    config = ConfigParser()
    config.read(BASE_DIR / 'config.ini', encoding='utf-8')
    return {
        'request': {
            'timeout': config.getint('REQUEST', 'Timeout', fallback=30),
            'retries': config.getint('REQUEST', 'MaxRetries', fallback=5)
        },
        'feishu': {
            'app_id': config['FEISHU']['AppID'],
            'app_secret': config['FEISHU']['AppSecret']
        }
    }