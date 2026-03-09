import json
import time
from kafka import KafkaProducer
import binance
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed

from config.settings import KAFKA_BROKER, KAFKA_TOPIC, SYMBOL
from schemas.trade_event import TradeEvent

client = binance.Client()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def send_to_kafka(message):
    producer.send(KAFKA_TOPIC, message)


def fetch_trades():
    trades = client.get_recent_trades(symbol=SYMBOL.upper())
    return trades


def main():

    logger.info("Starting Binance producer")

    while True:

        try:

            trades = fetch_trades()

            for trade in trades:

                event = TradeEvent(
                    symbol=SYMBOL,
                    price=float(trade["price"]),
                    quantity=float(trade["qty"]),
                    timestamp=int(trade["time"]),
                )

                send_to_kafka(event.dict())

            time.sleep(1)

        except Exception as e:
            logger.error(f"Producer error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
