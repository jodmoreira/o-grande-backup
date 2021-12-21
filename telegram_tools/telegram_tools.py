import telepot
from telepot.loop import MessageLoop
import os
import time

# Get token from enviroment variable
token = os.environ["TELEGRAM_BOT"]
chat_with_me = os.environ["TELEGRAM_BOT_CHAT_WITH_ME"]


def handle(msg):
    print(msg)


def send_message(bot, message):
    bot.sendMessage(chat_with_me, message)
    return


bot = telepot.Bot(token)
send_message(bot, "Bot on!")
MessageLoop(bot, handle).run_as_thread()
while 1:
    time.sleep(10)
