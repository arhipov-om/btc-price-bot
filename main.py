import asyncio
import json
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiohttp import ClientSession
from environs import env

env.read_env()

bot = Bot(token=env.str("BOT_TOKEN"), default=DefaultBotProperties(parse_mode="HTML"))
dispatcher = Dispatcher()

SUBSCRIBERS: dict[int, Message] = {}
SUBSCRIBERS_LOCK = asyncio.Lock()


async def update_message_task(price: float, message: Message):
    try:
        time = datetime.now().strftime('%d.%m.%Y, %H:%M:%S')
        await message.edit_text(text=f"BTC: ${price}\n{time}")
    except TelegramBadRequest:
        pass


async def broadcast():
    while True:
        try:
            async for tick in price_stream():
                price = json.loads(tick.data).get('p')
                async with SUBSCRIBERS_LOCK:
                    tasks = [update_message_task(price=price, message=message)
                             for user_id, message in SUBSCRIBERS.items()]
                    if tasks:
                        await asyncio.gather(*tasks)
                    await asyncio.sleep(5)
        except Exception:
            await asyncio.sleep(1)


async def price_stream():
    async with ClientSession() as session:
        async with session.ws_connect('wss://fstream.binance.com/ws/btcusdt@aggTrade') as ws:
            async for msg in ws:
                yield msg


@dispatcher.message(CommandStart)
async def process_start(message: Message):
    if not message.from_user.id in SUBSCRIBERS:
        msg = await message.answer("⏳ Подключаю к потоку цен...")
        SUBSCRIBERS[message.from_user.id] = msg


async def main():
    asyncio.create_task(broadcast())
    await dispatcher.start_polling(bot)


def run():
    asyncio.run(main())


if __name__ == '__main__':
    run()
