# these would generally be the wikis with a BRFA or similar - i.e, requires manual checks
import requests
from reminder import get_json_dict, send_messages


def get_wikidata_set(entity_id):
    S = requests.Session()
    URL = "https://wikidata.org/w/api.php"
    PARAMS = {
        "action": "wbgetentities",
        "ids":f"Q{entity_id}",
        "format": "json"
    }
    R = S.get(url=URL, params=PARAMS)
    DATA = R.json()
    wl = DATA['entities'][f'Q{entity_id}']['sitelinks']
    wikis = []
    for l in wl:
        wikis.append(l)

    return wikis

# gets the wikis that allow global bots
def get_gb_allowed():
    S = requests.Session()
    URL = "https://meta.wikimedia.org/w/api.php"
    PARAMS = {
        "action":"query",
        "format":"json",
        "list":"wikisets",
        "wsprop":"wikisincluded"
    }
    R = S.get(url=URL, params=PARAMS)
    DATA = R.json()
    wikis = []
    wl = DATA['query']['wikisets'][2]['wikisincluded'] # 2 = id 14, "Opted-out of global bots" but somewhat misleading name
    for l in wl:
        wikis.append(wl[l])

    return wikis

def generate_report():
    # get the list of wikis that we can consider
    gb_allowed = get_gb_allowed()
    # then consider wikis that we can already run
    approved_wikis = get_json_dict('Global_reminder_bot/global')['approved_wikis']
    # get a set of all wikis that require explicit authorisation
    cannot_run = get_wikidata_set('4615128')
    cannot_run += get_wikidata_set('8639023')
    cannot_run = set(cannot_run)
    # for each wiki in this, check whether it's already approved AND in the gb_allowed set
    cnt = 0
    invalid_set = []
    for crw in cannot_run:
        if crw in gb_allowed and crw not in approved_wikis:
            print(f"{crw} requires authorisation")
            cnt = cnt + 1
            invalid_set.append(crw)

    print(f"{cnt} wikis require authorisation")
    return invalid_set

def return_valid_wikis():
    # get the list of wikis that we can consider
    gb_allowed = get_gb_allowed()
    invalid_set = generate_report()
    approved_wikis = get_json_dict('Global_reminder_bot/global')['approved_wikis'] # don't want wikis that are already approved to avoid duplication
    valid_set = [x for x in gb_allowed if x not in invalid_set and x not in approved_wikis]
    return valid_set

def run_auto_approved_wikis():
    ls = get_json_dict('Global_reminder_bot/global')
    if 'auto_approval' in ls and ls['auto_approval']:
        wikis = return_valid_wikis()
        for w in wikis:
            send_messages(w)

if __name__ == "__main__":
    generate_report()