import asyncio
import logging
import os
import signal

import websockets
from google import pubsub_v1
from dotenv import load_dotenv

load_dotenv()

PUBSUB_TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.DEBUG
)
logger = logging.getLogger(__name__)


async def main():
    queue = asyncio.Queue()

    await asyncio.gather(
        producer(queue),
        consumer(queue),
    )


async def producer(queue: asyncio.Queue):
    while True:
        try:
            async for websocket in websockets.connect(
                "wss://stream.binance.com:9443/ws/btcusdt@trade"
            ):
                websocket.debug = False

                async for message in websocket:
                    queue.put_nowait(message)
        except Exception:
            logger.exception("An error has occurred at producer().")


async def consumer(queue: asyncio.Queue):
    pubsub_client = pubsub_v1.PublisherAsyncClient()

    while True:
        messages = []

        message: websockets.Data = await queue.get()
        if isinstance(message, str):
            messages.append(pubsub_v1.PubsubMessage(data=message.encode()))

        while not queue.empty():
            message: websockets.Data = queue.get_nowait()
            if isinstance(message, str):
                messages.append(pubsub_v1.PubsubMessage(data=message.encode()))

        try:
            await pubsub_client.publish(
                topic=PUBSUB_TOPIC_ID, messages=messages
            )
        except Exception:
            logger.exception("An error has occurred at consumer().")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
