from fastapi import FastAPI, Depends, HTTPException
from typing import List, Optional
from pydantic import BaseModel, parse_obj_as
from src.db_sql_alc import DB_alchemy
import config

DB_HOST = config.POSTGRE_HOST
DB_PORT = config.POSTGRE_PORT
DB_NAME = config.POSTGRE_DB_NAME
DB_USER_NAME = config.POSTGRE_DB_USER_NAME
DB_PWD = config.POSTGRE_DB_PWD


class Item(BaseModel):
    item_name: str
    description: Optional[str] = None
    price: float
    tax: Optional[float] = None


class ItemResponse(BaseModel):
    item_id: int
    item_name: str
    description: Optional[str] = None
    price: float
    tax: Optional[float] = None
    tax_with_price: float


app = FastAPI(title='FastAPI + Postgres', debug=True)

db_alc = DB_alchemy(config.POSTGRE_HOST,
                    config.POSTGRE_PORT,
                    config.POSTGRE_DB_NAME,
                    config.POSTGRE_DB_USER_NAME,
                    config.POSTGRE_DB_PWD)

db_alc.create_schema(config.POSTGRE_SCHEMA_NAME)
db_alc.create_metadata_obj(config.POSTGRE_SCHEMA_NAME)
items_table = db_alc.create_items_table()
db_alc.create_all()


def common_items(item_name: str, description: Optional[str],
                 price: float, tax: Optional[float], tax_with_price: float):
    return {'name': item_name, 'description': description, 'price': price,
            'tax': tax, 'tax_with_price': tax_with_price}


def find_item_by_name(itemName: str):
    query = items_table.select().where(
        items_table.c['item_name'] == itemName)
    queryResult = db_alc.db_engine.execute(query).fetchone()
    if queryResult is not None:
        return parse_obj_as(ItemResponse, dict(zip(queryResult.keys(), queryResult)))
    else:
        return 0


def find_item_by_id(itemId: int):
    query = items_table.select().where(
        items_table.c['item_id'] == itemId)
    queryResult = db_alc.db_engine.execute(query).fetchone()
    if queryResult is not None:
        return parse_obj_as(ItemResponse, dict(zip(queryResult.keys(), queryResult)))
    else:
        return 0


def delete_item(itemId: int):
    query = items_table.delete().where(
        items_table.c['item_id'] == itemId)
    db_alc.db_engine.execute(query)
    print(f'Item with ID {itemId} was successfully deleted')


def update_item(item: Item):
    updatedItem = items_table.update().where(
        items_table.c['item_name'] == item.item_name).values(
            description=item.description,
            price=item.price,
            tax=item.tax,
            tax_with_price=item.tax + item.price)
    queryResult = db_alc.db_engine.execute(
        updatedItem.returning(items_table))
    print(f'Updating Item ${item.item_name}')
    return queryResult.fetchone()


def insert_item(item: Item):
    newItem = items_table.insert().values(
        item_name=item.item_name,
        description=item.description,
        price=item.price,
        tax=item.tax,
        tax_with_price=item.tax + item.price
    )
    queryResult = db_alc.db_engine.execute(newItem.returning(items_table))
    print('A new item is added to a DB')
    return queryResult.fetchone()


@app.get('/')
def root():
    return {'message': 'FASTAPI with SqlAlchemy'}


@app.get('/items/id/{itemId}', response_model=ItemResponse)
# Get Item by ID
def get_item_by_id(itemId: int):
    ff = find_item_by_id(itemId)
    if ff == 0:
        raise HTTPException(
            status_code=404, detail=f'Item with ID {itemId} not found')
    else:
        return ff


@app.get('/items/name/{item_name}', response_model=ItemResponse)
# Get Item by Name
def read_item_by_name(item_name: str):
    ff = find_item_by_name(item_name)
    if ff == 0:
        raise HTTPException(
            status_code=404, detail=f'Item with {item_name} name not found')
    else:
        return ff


@app.put('/items/name/', response_model=ItemResponse)
# Update Item by Name
def update_item_by_name(item: Item):
    if find_item_by_name(item.item_name) == 0:
        raise HTTPException(
            status_code=404, detail=f'Item with {item.item_name} name not found')
    else:
        print(f'Updating the item ${item.item_name}')
        updatedItem = update_item(item)
        return {**item.dict(), 'item_id': updatedItem[0], 'tax_with_price': updatedItem[5]}


@app.post('/items/new/{item_name}', response_model=ItemResponse)
# Add a new item
# This request will be ASYNC
def create_item(item_name, item: Item):
    # Update the Item if it already exists
    if find_item_by_name(item_name) != 0:
        print(f'The item ${item_name} already exists, updating')
        return update_item(item)
    else:
        item.item_name = item_name
        newItem = insert_item(item)
        return ({**item.dict(), 'item_id': newItem[0],
                 'tax_with_price': newItem[5]})


@app.post('/items/new/', response_model=List[ItemResponse])
# Adding one or many items passed with a list
def create_item_list(itemList: List[Item]):
    result = []
    for item in itemList:
        # Ignore an item if it already exists
        if find_item_by_name(item.item_name) == 0:
            result.append(insert_item(item))
    print(len(result))
    if len(result) > 0:
        return [{'item_id': newItem[0],
                 'item_name': newItem[1],
                 'description': newItem[2],
                 'price': newItem[3],
                 'tax': newItem[4],
                 'tax_with_price': newItem[5]} for newItem in result]
    else:
        raise HTTPException(
            status_code=404, detail='No items to insert')


@app.delete('/items/delete/{item_id}')
def delete_item_by_id(item_id: int):
    if find_item_by_id(item_id) == 0:
        raise HTTPException(
            status_code=404, detail=f'Item with {item_id} ID not found')
    else:
        delete_item(item_id)
        return {'message': f'Item with {item_id} ID was deleted'}
