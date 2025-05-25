import asyncio
import json
import websockets
import aiohttp
import os

clients = set()

CONSUMER_API_URL = os.getenv("CONSUMER_API_URL", "http://consumer:5000")

async def handler(websocket):
    clients.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        clients.remove(websocket)

async def fetch_tables():
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{CONSUMER_API_URL}/tables") as resp:
            return await resp.json()

async def fetch_table_data(table_name):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{CONSUMER_API_URL}/tables/{table_name}") as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                return None

async def broadcast_updates():
    while True:
        try:
            table_names = await fetch_tables()
            for table_name in table_names:
                rows = await fetch_table_data(table_name)
                if rows is not None:
                    data = {
                        "category": table_name.replace("_", " ").title(),
                        "rows": rows
                    }
                    msg = json.dumps(data)
                    if clients:
                        await asyncio.gather(*(client.send(msg) for client in clients))
        except Exception as e:
            print(f"Error broadcasting: {e}")
        await asyncio.sleep(5)

async def main():
    async with websockets.serve(handler, "0.0.0.0", 8765):
        await broadcast_updates()

if __name__ == "__main__":
    asyncio.run(main())
