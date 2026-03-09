from pydantic import BaseModel


class TradeEvent(BaseModel):
    symbol: str
    price: float
    quantity: float
    timestamp: int
