from faust_kafka_stream import app, Item, ingestData, items_for_account


async def test_item():
    # start and stop the agent in this block
    async with ingestData.test_context() as agent:
        item = Item(item_id=1, item_name='some_item_name',
                    description='descr', price=300, tax=0.15, tax_with_price=300*0.15)
        # sent item to the test agents local channel, and wait
        # the agent to process it
        await agent.put(item)
        # at this point the agent already updated the table
        assert items_for_account[item.item_id] == 1
        await agent.put(item)
        assert items_for_account[item.item_id] == 2


async def run_tests():
    app.conf.store = 'memory://'   # tables must be in-memory
    await test_item()

if __name__ == '__main__':
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_tests())
