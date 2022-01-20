import db_tools.sqlite_tools as sqlite_tools
import db_tools.postgres_tools as postgres_tools
from datetime import date
import telegram_tools.telegram_tools as telegram_tools


def amount_of_tweets_stored_in_sheets_today():
    today_str = date.today().strftime("%Y-%m-%d")
    amount_of_tweets = sqlite_tools.amount_tweets_stored_in_sheets_by_date(today_str)
    telegram_tools.send_message(f"{amount_of_tweets} tweets stored in sheets today")


def amount_of_tweets_stored_in_s3_today():
    today_str = date.today().strftime("%Y-%m-%d")
    amount_of_tweets = postgres_tools.amount_tweets_stored_in_s3_by_date(today_str)
    print(amount_of_tweets)
    telegram_tools.send_message(f"{amount_of_tweets} tweets stored in s3 today")


if __name__ == "__main__":
    amount_of_tweets_stored_in_sheets_today()
    amount_of_tweets_stored_in_s3_today()
