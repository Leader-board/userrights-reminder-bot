# global bot reminder
import mysql.connector
import pandas as pd
import requests, json
from urllib.request import urlopen



def get_url(wiki_name):
    cnx = mysql.connector.connect(option_files='replica.my.cnf', host=f'{wiki_name}.analytics.db.svc.wikimedia.cloud',
                                  database=f'{wiki_name}_p')
    cursor = cnx.cursor()
    query = ("SELECT dbname, lang, family, name, url from wiki")
    cursor.execute(query)
    res = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    return res['url'].values[0]

def get_users_expiry_global():
    # uses a different table
    cnx = mysql.connector.connect(option_files='replica.my.cnf', host=f'centralauth.analytics.db.svc.wikimedia.cloud',
                                  database=f'centralauth_p')
    query="""
    SELECT ug.gug_user, u.gu_name, ug.gug_group, ug.gug_expiry from global_user_groups ug
    INNER JOIN globaluser u
    ON u.gu_id = ug.gug_user
    WHERE gug_expiry is not null
    AND gug_expiry < NOW() + INTERVAL 2 WEEK
    AND gug_expiry > NOW()
    """
    cursor = cnx.cursor()
    cursor.execute(query)
    res = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    print(res)
    cursor.close()
    return res




def get_users_expiry(wiki_name):
    cnx = mysql.connector.connect(option_files='replica.my.cnf', host=f'{wiki_name}.analytics.db.svc.wikimedia.cloud',
                                  database=f'{wiki_name}_p')
    query="""
    SELECT ug.ug_user as userid, u.user_name as username, ug.ug_group as userright, ug.ug_expiry as expiry from user_groups ug
    INNER JOIN user u
    ON u.user_id = ug.ug_user
    WHERE ug_expiry is not null
    AND ug_expiry < NOW() + INTERVAL 2 WEEK
    AND ug_expiry > NOW()
    """
    cursor = cnx.cursor()
    cursor.execute(query)
    res = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    print(res)
    cursor.close()
    return res

def get_token(wiki_url):
    S = requests.Session()

    f = open("../../botdetails.txt", "r")
    filecont = f.read().splitlines()
    f.close()
    if len(filecont) != 5:
        print("The botdetails file is not in the expected format")
        return
    # Step 1: GET request to fetch login token
    PARAMS_0 = {
        "action": "query",
        "meta": "tokens",
        "type": "login",
        "format": "json"
    }
    # URL = r'https://meta.wikimedia.org/w/api.php'
    URL = f"{wiki_url}/w/api.php"
    R = S.get(url=URL, params=PARAMS_0)
    DATA = R.json()

    LOGIN_TOKEN = DATA['query']['tokens']['logintoken']

    # Step 2: POST request to log in. Use of main account for login is not
    # supported. Obtain credentials via Special:BotPasswords
    # (https://www.mediawiki.org/wiki/Special:BotPasswords) for lgname & lgpassword
    PARAMS_1 = {
        "action": "login",
        "lgname": filecont[1],
        "lgpassword": filecont[2],
        "lgtoken": LOGIN_TOKEN,
        "format": "json"
    }

    R = S.post(URL, data=PARAMS_1)

    # Step 3: GET request to fetch CSRF token
    PARAMS_2 = {
        "action": "query",
        "meta": "tokens",
        "format": "json"
    }

    R = S.get(url=URL, params=PARAMS_2)
    DATA = R.json()

    CSRF_TOKEN = DATA['query']['tokens']['csrftoken']

    return CSRF_TOKEN, URL, S, filecont[4]

def get_wiki_url(wiki_name):
    cnx = mysql.connector.connect(option_files='/root/replica.my.cnf', host='meta.analytics.db.svc.wikimedia.cloud',
                                  database='meta_p')
    cursor = cnx.cursor()
    query = ("""
    SELECT dbname, lang, family, name, url from wiki WHERE name = {}
    """.format(wiki_name))

    cursor.execute(query)
    res = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    return res['url'].values[0]

def get_json_dict(page_name):
    # this will ALWAYS be on Meta-Wiki (either production or beta cluster
    #url = r'https://meta.wikimedia.org/w/api.php?action=parse&formatversion=2&page='
    starting_url = r'https://meta.wikimedia.beta.wmflabs.org/w/api.php?action=parse&formatversion=2&page='
    url = starting_url + page_name + r'/Mailing+list&prop=wikitext&format=json'
    # get the json
    response = urlopen(url)
    data_json = json.loads(response.read())
    main_data = json.loads(data_json['parse']['wikitext']) # this is the actual JSON

    return main_data

def prepare_message(wiki_name, user_name, user_right, user_expiry):
    # we assume that the wiki is in the allowlist
    # get the LOCAL and GLOBAL jsons
    global_data = get_json_dict('T370842/global')
    local_data = get_json_dict(f'T370842/{wiki_name}')
    # check if the right is in the global OR local exclusion lists
    global_exclude = global_data['always_excluded_local']
    local_exclude = local_data['always_excluded']
    if user_right in global_exclude or user_right in local_exclude:
        return # do NOT proceed
    # then determine the base message to send
    message_to_send = global_data['text']['default']
    if user_right in local_data['text']:
        message_to_send = local_data['text'][user_right]

    # replace the $1 and $2
    message_to_send = message_to_send.replace("$1", user_right)
    message_to_send = message_to_send.replace("$2", user_expiry)

    title_to_send = local_data['title'] # for now assume that local page exists - TODO this

    # and then we can send!
    inform_users(wiki_name, user_name, title_to_send, message_to_send)

def get_opt_out():
    # later on
    pass

def user_expiry_database_load():
    # JSON database stored on-wiki
    # per user: [user_name, user_right, user_expiry]
    pass


def send_messages(wiki_name):
    users = get_users_expiry(wiki_name)
    # TODO iterrows is a very bad idea so we need to move away from it as soon as possible
    for row in users.itertuples(index=True, name='Pandas'):
        prepare_message(wiki_name, row.username, row.userright, row.expiry)




def inform_users(wiki_name, user, title, message):
    # inform users that their user right will expire soon
    # find the user talk page
    wiki_url = get_url(wiki_name)
    CSRF_TOKEN, URL, S, api_link = get_token(wiki_url)
    # we need to create a new section on that user
    # Step 4: POST request to edit a page
    PARAMS_3 = {
        "action": "edit",
        "title": f"User talk:{user}",
        "section": "new",
        "sectiontitle": title,
        "token": CSRF_TOKEN,
        "format": "json",
        "appendtext": message
    }
    R = S.post(URL, data=PARAMS_3)
    DATA = R.json()

    print(DATA)





#get_users_expiry('wikifunctionswiki')
send_messages('testwiki')
get_users_expiry_global()