# utils.py for scraping player info from college softball rosters

import random
from config import GEMINI_API_KEYS

def get_random_api_key():
    return random.choice(GEMINI_API_KEYS)