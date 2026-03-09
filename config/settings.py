import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-trades")

SYMBOL = os.getenv("SYMBOL", "btcusdt")

DATA_PATH = os.getenv("DATA_PATH", "data/ohlc/")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "checkpoints/ohlc/")
