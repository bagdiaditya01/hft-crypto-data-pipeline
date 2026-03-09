import pandas as pd
from loguru import logger

from config.settings import DATA_PATH


def generate_signal(df):

    df["ma_short"] = df["close"].rolling(window=5).mean()
    df["ma_long"] = df["close"].rolling(window=20).mean()

    latest = df.iloc[-1]

    if latest["ma_short"] > latest["ma_long"]:
        return "BUY"

    elif latest["ma_short"] < latest["ma_long"]:
        return "SELL"

    return "HOLD"


def main():

    logger.info("Generating signals")

    df = pd.read_parquet(DATA_PATH)

    signal = generate_signal(df)

    logger.info(f"Trading signal: {signal}")


if __name__ == "__main__":
    main()
