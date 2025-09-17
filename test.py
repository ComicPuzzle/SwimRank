import asyncio

async def async_task():
    print("Async task starting...")
    await asyncio.sleep(2)
    print("Async task finished.")

async def main_async():
    print("Main async starting...")
    await async_task()
    print("Main async continuing after task.")

asyncio.run(main_async())