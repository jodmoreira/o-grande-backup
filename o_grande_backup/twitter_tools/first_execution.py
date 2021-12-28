import main_twitter_ogb
import database
import os


def execute(screen_name):
    # try:
    user_id = main_twitter_ogb.get_user_id(screen_name)
    data = (screen_name, user_id)
    new_profile = database.add_new_profile(data)
    profiles = database.read_profiles()
    # except:
    #     print("erro 1")
    if new_profile == "Done!":
        try:
            main_twitter_ogb.primeira_execucao(screen_name)
        except:
            print("erro 2")
            pass
    print(new_profile)


if __name__ == "__main__":
    execute("jodmoreira")
