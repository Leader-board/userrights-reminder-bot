# global bot reminder
import re

import mysql.connector
import pandas as pd
import requests, json
from urllib.request import urlopen
from dateutil import parser
import ast




def get_url(wiki_name):
    cnx = mysql.connector.connect(option_files='replica.my.cnf', host=f'meta.analytics.db.svc.wikimedia.cloud',
                                  database=f'meta_p')
    cursor = cnx.cursor()
    query = ("""
    SELECT dbname, lang, family, name, url from wiki WHERE dbname = '{wiki_name}'
    """.format(wiki_name = wiki_name))
    cursor.execute(query)
    res = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    return res['url'].values[0]

def get_users_expiry_global():
    # uses a different table
    cnx = mysql.connector.connect(option_files='replica.my.cnf', host=f'centralauth.analytics.db.svc.wikimedia.cloud',
                                  database=f'centralauth_p')
    query="""
    SELECT ug.gug_user as userid, u.gu_name as username, ug.gug_group as userright, ug.gug_expiry as expiry from global_user_groups ug
    INNER JOIN globaluser u
    ON u.gu_id = ug.gug_user
    WHERE gug_expiry is not null
    AND gug_expiry < NOW() + INTERVAL 1 WEEK
    AND gug_expiry > NOW()
    """
    cursor = cnx.cursor()
    cursor.execute(query)
    res = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    print(res)
    cursor.close()
    return res

def get_wiki_usergroup(mw_name, wiki_name):
    # to map, for instance, Wikiversity's sysop (which is actually called curator)
    val = get_json_dict(f'MediaWiki:Group-{mw_name}', get_url(wiki_name))
    return val # this is a string


def get_users_expiry(wiki_name):
    cnx = mysql.connector.connect(option_files='replica.my.cnf', host=f'{wiki_name}.analytics.db.svc.wikimedia.cloud',
                                  database=f'{wiki_name}_p')
    query="""
    SELECT ug.ug_user as userid, u.user_name as username, ug.ug_group as userright, ug.ug_expiry as expiry from user_groups ug
    INNER JOIN user u
    ON u.user_id = ug.ug_user
    WHERE ug_expiry is not null
    AND ug_expiry < NOW() + INTERVAL 1 WEEK
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

    f = open("../botdetails.txt", "r")
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

def get_json_dict(page_name, wiki_link = r'https://meta.wikimedia.org'):
    # this will ALWAYS be on Meta-Wiki (either production or beta cluster
    #url = r'https://meta.wikimedia.org/w/api.php?action=parse&formatversion=2&page='
    starting_url = wiki_link + r'/w/api.php?action=parse&formatversion=2&page='
    url = starting_url + page_name + r'&prop=wikitext&format=json'
#    print(url)
    # get the json
    response = urlopen(url)
    # https://stackoverflow.com/questions/39491420/python-jsonexpecting-property-name-enclosed-in-double-quotes
    data_json = json.loads(response.read())
    # print(f"page name = {page_name}")
    if 'error' in data_json:
        return None # does not exist
    try:
        main_data = json.loads(data_json['parse']['wikitext']) # this is the actual JSON
    except ValueError as e:
        main_data = data_json['parse']['wikitext'] # not JSON
    return main_data

def prepare_message(wiki_name, user_name, user_right, user_expiry):
    # we assume that the wiki is in the allowlist
    # get the LOCAL and GLOBAL jsons
    global_data = get_json_dict('Global_reminder_bot/global')
    if wiki_name != 'global':
        local_data = get_json_dict(f'Global_reminder_bot/{wiki_name}')
    else:
        local_data = None # does not apply for global rights

    local_database = get_json_dict('Global_reminder_bot/database')
    if user_name in get_opt_out():
        return # user has chosen to exclude themselves
    elif wiki_name not in local_database:
        pass
    elif user_expiry not in local_database[wiki_name]:
        pass
    else:
        # get the list
        ll = local_database[wiki_name][user_expiry]
        # reminder: [user_name, user_right]
        exists = False
        for det in ll:
            if (det[0] == user_name and det[1] == user_right):
                # we found it
                exists = True
                break

        if exists:
            # do not process this - already in database
            return

    print(local_data)
    local_exists = True
    if local_data is None:
        local_exists = False
    # check if the right is in the global OR local exclusion lists
    global_exclude = global_data['always_excluded_local']
    global_rights_exclude = global_data['always_excluded_global']
    if local_exists:
        local_exclude = local_data['always_excluded']
    else:
        local_exclude = None
    if (wiki_name != 'global' and user_right in global_exclude) or (local_exists and (user_right in local_exclude)) or (wiki_name == 'global' and user_right in global_rights_exclude):
        return # do NOT proceed
    # then determine the base message to send
    if wiki_name != 'global':
        message_to_send = global_data['text']['default']
        if local_exists and (user_right in local_data['text']):
            message_to_send = local_data['text'][user_right]
        elif local_exists:
            message_to_send = local_data['text']['default']
    else:
        message_to_send = global_data['text']['default_global']

    # make user_expiry human-readable
    ts = parser.parse(user_expiry)
    expiry_fmt = ts.strftime('%Y-%m-%d %H:%M:%S')

    # replace the $n where applicable
    message_to_send = message_to_send.replace("$1", user_right)
    if wiki_name != 'global' and get_wiki_usergroup(user_right, wiki_name) is not None:
        message_to_send = message_to_send.replace("$2", get_wiki_usergroup(user_right, wiki_name))
    else:
        message_to_send = message_to_send.replace("($2)", '')
    message_to_send = message_to_send.replace("$3", expiry_fmt)

    if local_exists and wiki_name != 'global':
        title_to_send = local_data['title']['default']
    else:
        title_to_send = global_data['title']['default']

    title_to_send = title_to_send.replace("$1", user_right)
    # and then we can send!
    inform_users(wiki_name, user_name, title_to_send, message_to_send)

    # after sending, add its entry in database
    if wiki_name not in local_database:
        local_database[wiki_name] = {}
    if user_expiry not in local_database[wiki_name]:
        local_database[wiki_name][user_expiry] = []

    ll = local_database[wiki_name][user_expiry]
    ll.append([user_name, user_right])
    local_database[wiki_name][user_expiry] = ll

    # convert that to json and put it back

    user_expiry_database_save(local_database)

def get_opt_out():
    # later on
    ll = get_json_dict('Global_reminder_bot/Exclusion')['targets']
    excluded_users = []
    #print(ll)
    for d in ll:
       # print(d)
        excluded_users.append(re.split('[:\/]', d['title'])[1])

    return excluded_users

def user_expiry_database_load():
    # JSON database stored on-wiki
    # [wiki] -> [{expiry_date -> [{user, user_right}]]
    db = get_json_dict('Global_reminder_bot/database')
    return db

def user_expiry_database_save(db):
    r = json.dumps(db)
    # save that to db
    wiki_url = 'https://meta.wikimedia.org'
    CSRF_TOKEN, URL, S, api_link = get_token(wiki_url)
    PARAMS_3 = {
        "action": "edit",
        "title": "Global reminder bot/database",
        "contentmodel": "json",
        "token": CSRF_TOKEN,
        "format": "json",
        "text": r
    }
    R = S.post(URL, data=PARAMS_3)
    print(R.content)
    DATA = R.json()

    print(DATA)


def send_messages(wiki_name):
    if wiki_name != 'global':
        users = get_users_expiry(wiki_name)
    else:
        users = get_users_expiry_global()

    for row in users.itertuples(index=True, name='Pandas'):
        # IMPORTANT: only Leaderbot works on testwiki!
        if (wiki_name != 'testwiki' and wiki_name != 'global') or row.username.decode("utf-8") == 'Leaderbot' and 'WMF' not in row.username.decode("utf-8"):
            prepare_message(wiki_name, row.username.decode("utf-8"), row.userright.decode("utf-8"), row.expiry.decode("utf-8"))




def inform_users(wiki_name, user, title, message):
    # inform users that their user right will expire soon
    # find the user talk page
    if wiki_name != 'global':
        wiki_url = get_url(wiki_name)
    else:
        wiki_url = get_url('metawiki')
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


# wikis to run
send_messages('wikifunctionswiki')
send_messages('enwikibooks')
send_messages('frwiki')
send_messages('eswiki')
send_messages('wikimaniawiki')
send_messages('enwikivoyage')
send_messages('enwiki')

# send_messages('global')

# send_messages('metawiki')
# send_messages('mediawikiwiki')
# send_messages('incubatorwiki')
# send_messages('enwikibooks')

get_users_expiry_global()