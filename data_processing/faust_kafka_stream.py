import faust
from mode import Service
from send_request import send_request_new_item
import config
import sys
import time


class Item(faust.Record, serializer='json'):
    # This is to deserialize data
    item_id: int
    item_name: str
    description: str
    price: float
    tax: float
    tax_with_price: float


def new_item_record(responseJson):
    # This method will create a new record based on a request response
    return Item(item_id=responseJson.get("item_id"),
                item_name=responseJson.get("item_name"),
                description=responseJson.get("description"),
                price=responseJson.get("price"),
                tax=responseJson.get("tax"),
                tax_with_price=responseJson.get("tax_with_price"))


# Creating a new Faust App and a Kafka topic
app = faust.App('faust-app', broker=config.KAFKA_BOOTSTRAP_SERVER)
items_topic = app.topic(config.KAFKA_TOPIC, key_type=str, value_type=Item)
items_for_account = app.Table('items-count-by-account', default=int)


@app.service
class MyService(Service):

    async def on_start(self):
        print('KAFKA_FAUST IS STARTING')

    async def on_stop(self):
        print('KAFKA_FAUST IS STOPPING')

    @Service.task
    async def _background_task(self):
        while not self.should_stop:
            print('KAFKA_FAUST BACKGROUND TASK every 20 seconds')
            await self.sleep(20.0)


@app.agent(items_topic)
# Creating an agent, which will (in another method)
# receive data from a request and send it to Kafka topic
async def ingestData(items):
    print('AGENT STARTED')
    async for item in items:
        print(f'Processing a new item {item}')
        yield item


# @app.timer(interval=5.0) # Interval task to send a request and send received response to an agent
@app.task  # This task will send a request and then will its response to the agent
async def send_new_item_record(app: faust.app):
    time.sleep(10)
    response = send_request_new_item()
    item = new_item_record(response)
    await ingestData.send(key='MYkey', value=item)
    time.sleep(20)
    sys.exit(0)

if __name__ == '__main__':
    app.main()
