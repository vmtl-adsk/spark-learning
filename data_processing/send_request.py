import requests
import json
import random as rnd
import config
from essential_generators import DocumentGenerator

dg = DocumentGenerator()


def send_request_new_item():
    item_name = dg.slug()
    jsn = {
        "item_name": item_name,
        "description": dg.sentence(),
        "price": rnd.randint(1, 10000),
        "tax": rnd.randint(1, 35) / 100
    }

    HEADERS = {
        'Accept-Encoding': 'gzip, deflate, br',
        'accept': '*/*',
        'Content-Type': 'application/json'
    }

    jsnObj = json.dumps(jsn)

    url = f'http://fastapi:{config.UVICORN_PORT}/items/new/{item_name}'
    rsp = requests.post(url=url, data=jsnObj, headers=HEADERS)

    print(f'{rsp}')
    return rsp.json()
