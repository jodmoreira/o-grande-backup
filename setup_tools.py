import aws_tools.s3_tools as s3_tools
import db_tools.db_tools as db_tools
import wf_tools.wf_tools as wf_tools
import json


def add_agents_to_db():
    all_sites = eval(wf_tools.get_all_sites())
    for site in all_sites:
        if site["name"] == "ogb":
            site_id = site["_id"]
        else:
            pass
    all_collections = eval(wf_tools.get_all_collections(site_id))
    for collection in all_collections:
        if collection["name"] == "Agents":
            collection_id = collection["_id"]
        else:
            pass
    count = 0
    total = 1
    agents_output = list()
    while count < total:
        all_items = wf_tools.get_all_items_from_collections(site_id, collection_id, offset=count)
        all_items = json.loads(all_items)
        count = all_items['offset']+100
        total = all_items['total']
        for i in all_items['items']:
            try:
                agent_names = {'name':i['name'], 'twitter_address':i['endereco-do-twitter']}
            except:
                pass
        # agent_names = [ {'name':i for i in all_items["items"]]
            agents_output += agent_names
    return agents_output

def add_twitter_profile_to_db():
    all_sites = eval(wf_tools.get_all_sites())
    for site in all_sites:
        if site["name"] == "ogb":
            site_id = site["_id"]
        else:
            pass
    all_collections = eval(wf_tools.get_all_collections(site_id))
    for collection in all_collections:
        if collection["name"] == "Agents":
            collection_id = collection["_id"]
        else:
            pass
    count = 0
    total = 1
    twitter_output = list()
    while count < total:
        all_items = wf_tools.get_all_items_from_collections(site_id, collection_id, offset=count)
        all_items = json.loads(all_items)
        count = all_items['offset']+100
        total = all_items['total']
        twitter_names = list()
        for i in all_items["items"]:
            try:
                dict_loop = {'name': i["name"], 'twitter': i["endereco-do-twitter"].replace('https://twitter.com/','')}
                twitter_names.append(dict_loop)
            except:
                pass
        twitter_output += twitter_names
    return twitter_output


if __name__ == "__main__":
    all_twitter_profiles = add_twitter_profile_to_db()
    # all_profiles = add_agents_to_db()
    arq_entrada = open('backup.csv','r')
    arq_entrada = arq_entrada.readlines()
    for agent_backup in arq_entrada:
        agent_backup = agent_backup.replace('\n','')
        agent_backup = agent_backup.split(',')
        for agent in all_twitter_profiles:
            if agent['twitter'].lower() == agent_backup[0].lower():
                output = {
                    'name':agent['name'],
                    'screen_name' : agent['twitter'],
                    'twitter_id' : agent_backup[1]
                    }
                print(output)
                db_tools.add_new_twitter_profile(agent['name'],agent['twitter'],agent_backup[1])