import pandas as pd
from loguru import logger

from config.settings import DATA_PATH


def main():

    df = pd.read_parquet(DATA_PATH)

    logger.info("Latest OHLC records")
    logger.info(df.tail())


if __name__ == "__main__":
    main()