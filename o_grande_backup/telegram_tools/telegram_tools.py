# Telepot is no longer maintained, so i need to replace it with a new one.
# Maybe https://docs.telethon.dev/en/stable/
import telepot
from telepot.loop import MessageLoop
import os
import time

# Get token from enviroment variable
TOKEN = os.environ["TELEGRAM_BOT"]
CHAT_WITH_ME_ID = os.environ["TELEGRAM_BOT_CHAT_WITH_ME"]
BOT = telepot.Bot(TOKEN)


def handle(msg):
    print(msg)


def send_message(message):
    BOT.sendMessage(CHAT_WITH_ME_ID, message)
    return


# send_message(BOT, "Bot on!")
# MessageLoop(BOT, handle).run_as_thread()
# while 1:
#     time.sleep(10)
