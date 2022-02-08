## Report system to keep me updated about the system functions
import db_tools.sqlite_tools as sqlite_tools
import db_tools.postgres_tools as postgres_tools
from datetime import date
import telegram_tools.telegram_tools as telegram_tools


def amount_of_tweets_stored_in_sheets_today():
    today_str = date.today().strftime("%Y-%m-%d")
    amount_of_tweets = sqlite_tools.amount_tweets_stored_in_sheets_by_date(today_str)
    telegram_tools.send_message(f"{amount_of_tweets} tweets stored in sheets today")
    return amount_of_tweets


def amount_of_tweets_stored_in_sheets():
    amount_of_tweets = sqlite_tools.amount_tweets_stored_in_sheets()
    telegram_tools.send_message(f"{amount_of_tweets} tweets stored in sheets")
    return amount_of_tweets


def amount_of_tweets_stored_in_s3_today():
    today_str = date.today().strftime("%Y-%m-%d")
    amount_of_tweets = postgres_tools.amount_tweets_stored_in_s3_by_date(today_str)
    telegram_tools.send_message(f"{amount_of_tweets} tweets stored in s3 today")
    return amount_of_tweets


def amount_of_tweets_stored_in_s3():
    amount_of_tweets = postgres_tools.amount_tweets_stored_in_s3()
    telegram_tools.send_message(f"{amount_of_tweets} tweets stored in s3")
    return amount_of_tweets


if __name__ == "__main__":
    tweets_sheets = amount_of_tweets_stored_in_sheets_today()
    tweets_s3 = amount_of_tweets_stored_in_s3_today()
    amount_of_tweets_stored_in_sheets()
    amount_of_tweets_stored_in_s3()

    print(
        f"Done! {tweets_sheets} tweets stored in sheets and {tweets_s3} tweets stored in s3"
    )
