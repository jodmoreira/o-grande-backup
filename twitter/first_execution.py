import main_twitter_ogb
import database




screen_name = 'sensacionalista'
user_id = main_twitter_ogb.get_user_id(screen_name)
data = (screen_name, user_id)
database.add_new_profile(data)
profiles = database.read_profiles()
print(profiles)