import db_tools.sqlite_tools as sqlite_tools
from datetime import date
import telegram_tools.telegram_tools as telegram_tools


def amount_of_tweets_stored_in_sheets_today():
    today_str = date.today().strftime("%Y-%m-%d")
    amount_of_tweets = sqlite_tools.amount_tweets_stored_in_sheets_by_date(today_str)
    telegram_tools.send_message(f"{amount_of_tweets} tweets stored in sheets today")


if __name__ == "__main__":
    amount_of_tweets_stored_in_sheets_today()
