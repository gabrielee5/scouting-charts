import ccxt
from dotenv import dotenv_values
import os
import logging
from pybit.unified_trading import HTTP
from datetime import datetime

secrets = dotenv_values(".env")
api_key = secrets["BYBIT_API_KEY"]
api_secret = secrets["BYBIT_API_SECRET"]

session = HTTP(
        api_key=api_key, 
        api_secret=api_secret
        )
