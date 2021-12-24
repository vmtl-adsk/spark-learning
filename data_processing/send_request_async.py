import aiohttp
import json
import random as rnd
import config
from essential_generators import DocumentGenerator

dg = DocumentGenerator()


async def send_request_async():
    # This part returns None :( so using regular request lib
    item_name = dg.slug()
    jsn = {
        "item_name": item_name,
        "description": dg.sentence(),
        "price": rnd.randint(1, 10000),
        "tax": rnd.randint(1, 35) / 100
    }

    jsnObj = json.dumps(jsn)
    url = f'http://{config.UVICORN_HOST}:{config.UVICORN_PORT}/items/new/{item_name}'
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=jsnObj) as response:
            return await response.json()
