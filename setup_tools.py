import aws_tools.s3_tools as s3_tools

# import db_tools.db_tools as db_tools
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
        if collection["name"] == "Agentes":
            collection_id = collection["_id"]
        else:
            pass
    all_items = wf_tools.get_all_items_from_collections(site_id, collection_id)
    all_items = json.loads(all_items)
    agent_names = [i["name"] for i in all_items["items"]]
    print(agent_names)


if __name__ == "__main__":
    all_agents = add_agents_to_db()
