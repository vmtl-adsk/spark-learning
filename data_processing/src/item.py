from dataclasses import dataclass


@dataclass(frozen=True)
class Item:
    item_id: int()
    item_name: str()
    description: str()
    price: float()
    price_with_tax: float()
