# global bot reminder
import argparse as ap
import re

import mysql.connector
import pandas as pd
import requests, json
from urllib.request import urlopen
from dateutil import parser
from babel.dates import format_datetime

import vars
import wikilist
import traceback


only_update_db = True


def get_url(wiki_name):
    cnx = mysql.connector.connect(option_files='replica.my.cnf', host=f'meta.analytics.db.svc.wikimedia.cloud',
                                  database=f'meta_p', charset='utf8', use_unicode=True)
    cursor = cnx.cursor()
    query = ("""
    SELECT CONVERT(dbname using utf8) as dbname, CONVERT(lang using utf8) as lang, CONVERT(family using utf8) as family, CONVERT(name using utf8) as name, CONVERT(url using utf8) as url from wiki WHERE dbname = '{wiki_name}'
    """.format(wiki_name=wiki_name))
    cursor.execute(query)
    res = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    return res['url'].values[0]


def get_users_expiry_global(interval = 1, lower_bound = 25):
    # uses a different table
    cnx = mysql.connector.connect(option_files='replica.my.cnf', host=f'centralauth.analytics.db.svc.wikimedia.cloud',
                                  database=f'centralauth_p', charset='utf8', use_unicode=True)
    query = """
    SELECT CONVERT(ug.gug_user using utf8) as userid, CONVERT(u.gu_name using utf8) as username, CONVERT(ug.gug_group using utf8) as userright, ug.gug_expiry as expiry from global_user_groups ug
    INNER JOIN globaluser u
    ON u.gu_id = ug.gug_user
    WHERE gug_expiry is not null
    AND gug_expiry <= NOW() + INTERVAL {interval} WEEK
    AND gug_expiry >= NOW() + INTERVAL {lower_bound} HOUR
    """.format(interval = interval, lower_bound = lower_bound)
    cursor = cnx.cursor()
    cursor.execute(query)
    res = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    print(res)
    vars.current_stream = vars.current_stream + res.to_string() + '\n'
    cursor.close()
    return res


def get_wiki_usergroup(mw_name, wiki_name):
    # to map, for instance, Wikiversity's sysop (which is actually called curator)
    val = get_json_dict(f'MediaWiki:Group-{mw_name}', get_url(wiki_name))

    if val is None:
        # get name from database
        val = get_message_name(mw_name, get_wiki_lang(wiki_name))


    return val  # this is a string

def get_message_name(mw_name, wiki_lang):
    # does not require login
    S = requests.Session()

    URL = "https://meta.wikimedia.org/w/api.php"

    PARAMS = {
        "action": "query",
        "meta": "allmessages",
        'ammessages': f"group-{mw_name}",
        "amlang": wiki_lang,
        "format": "json"
    }

    R = S.get(url=URL, params=PARAMS)
    DATA = R.json()
    rr =  DATA['query']['allmessages'][0] # we asked for only one message
    # print(rr)
    if '*' not in rr or rr['*'] == '-':
        return None
    else:
        return rr['*'] # '*' is the display name


def get_users_expiry(wiki_name, interval = 1, lower_bound = 25):
    try:
        cnx = mysql.connector.connect(option_files='replica.my.cnf', host=f'{wiki_name}.analytics.db.svc.wikimedia.cloud',
                                      database=f'{wiki_name}_p', charset='utf8', use_unicode=True)
        query = """
        SELECT ug.ug_user as userid, CONVERT(u.user_name USING utf8) as username, CONVERT(ug.ug_group USING utf8) as userright, ug.ug_expiry as expiry from user_groups ug
        INNER JOIN user u
        ON u.user_id = ug.ug_user
        WHERE ug_expiry is not null
        AND ug_expiry < NOW() + INTERVAL {interval} WEEK
        AND ug_expiry > NOW() + INTERVAL {lower_bound} HOUR
        """.format(interval = interval, lower_bound = lower_bound)
        cursor = cnx.cursor()
        cursor.execute(query)
        res = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        print(res)
        vars.current_stream = vars.current_stream + res.to_string() + '\n'
        cursor.close()
        return res
    except Exception as e:
        print(e)
        vars.current_stream = vars.current_stream + str(e)
        return None


def send_central_logging():
    # send the logs up to Meta-Wiki
    r = json.dumps(vars.central_log)
    # save that to db
    wiki_url = 'https://meta.wikimedia.org'
    CSRF_TOKEN, URL, S, api_link = get_token(wiki_url)
    PARAMS_3 = {
        "action": "edit",
        "title": "Global reminder bot/log",
        "contentmodel": "json",
        "bot": "yes",
        "token": CSRF_TOKEN,
        "format": "json",
        "text": r
    }
    R = S.post(URL, data=PARAMS_3)




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


def load_meta_p(wiki_name):
    cnx = mysql.connector.connect(option_files='replica.my.cnf', host='meta.analytics.db.svc.wikimedia.cloud',
                                  database='meta_p', charset='utf8', use_unicode=True)
    cursor = cnx.cursor()
    query = ("""
    SELECT CONVERT(dbname using utf8) as dbname, CONVERT(lang using utf8) as lang, CONVERT(family using utf8) as family, CONVERT(name using utf8) as name, CONVERT(url using utf8) as url from wiki WHERE dbname = '{}'
    """.format(wiki_name))

    cursor.execute(query)
    res = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    return res

def get_wiki_url(wiki_name):
    res = load_meta_p(wiki_name)
    return res['url'].values[0]

def get_wiki_lang(wiki_name):
    res = load_meta_p(wiki_name)
    return res['lang'].values[0]


def get_json_dict(page_name, wiki_link=r'https://meta.wikimedia.org'):
    # this will ALWAYS be on Meta-Wiki (either production or beta cluster
    # url = r'https://meta.wikimedia.org/w/api.php?action=parse&formatversion=2&page='
    starting_url = wiki_link + r'/w/api.php?action=parse&formatversion=2&page='
    url = starting_url + page_name + r'&prop=wikitext&format=json'
    #    print(url)
    # get the json
    response = urlopen(url)
    # https://stackoverflow.com/questions/39491420/python-jsonexpecting-property-name-enclosed-in-double-quotes
    data_json = json.loads(response.read())
    # print(f"page name = {page_name}")
    if 'error' in data_json:
        return None  # does not exist
    try:
        main_data = json.loads(data_json['parse']['wikitext'])  # this is the actual JSON
    except ValueError as e:
        main_data = data_json['parse']['wikitext']  # not JSON
    return main_data


def prepare_message(wiki_name, user_name, user_right, user_expiry, user_id):
    # we assume that the wiki is in the allowlist
    # get the LOCAL and GLOBAL jsons
    global_data = get_json_dict('Global_reminder_bot/global')
    if wiki_name != 'global':
        local_data = get_json_dict(f'Global_reminder_bot/{wiki_name}')
    else:
        local_data = None  # does not apply for global rights

    local_database = get_json_dict('Global_reminder_bot/database')
    if user_name in get_opt_out():
        return  # user has chosen to exclude themselves
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
            # LEGACY: when the username was used for checking
            if len(det) == 2 and det[0] == user_name and det[1] == user_right:
                # we found it
                exists = True
                break
            # we now compare by user ID, avoiding issues with rename
            elif len(det) == 3 and det[2] == user_id and det[1] == user_right:
                exists = True
                break

        if exists:
            # do not process this - already in database
            return

    #print(local_data)
    local_exists = True
    if local_data is None:
        local_exists = False
    # check if the right is in the global OR local exclusion lists
    global_exclude = global_data['always_excluded_local']
    global_rights_exclude = global_data['always_excluded_global']
    if local_exists and 'always_excluded' in local_data:
        local_exclude = local_data['always_excluded']
    else:
        local_exclude = None

    # handle date formatting and locales

    if local_exists and 'date_format' in local_data:
        date_format = local_data['date_format']
    else:
        date_format = global_data['default_date_format']

    if local_exists and 'date_locale' in local_data:
        date_locale = local_data['date_locale']
    else:
        date_locale = global_data['default_date_locale']

    if (wiki_name != 'global' and user_right in global_exclude) or (local_exists and (user_right in local_exclude)) or (
            wiki_name == 'global' and user_right in global_rights_exclude):
        return  # do NOT proceed
    # then determine the base message to send
    if wiki_name != 'global':
        message_to_send = global_data['text']['default']
        if local_exists and 'text' in local_data and (user_right in local_data['text']):
            message_to_send = local_data['text'][user_right]
        elif local_exists and 'text' in local_data:
            # some wikis may not choose to even have default
            if 'default' in local_data['text']:
                message_to_send = local_data['text']['default']
            else:
                message_to_send = global_data['text']['default']
    else:
        message_to_send = global_data['text']['default_global']

    # make user_expiry human-readable
    ts = parser.parse(user_expiry)
    expiry_fmt = format_datetime(ts,date_format,locale=date_locale)

    # replace the $n where applicable
    message_to_send = message_to_send.replace("$1", user_right)
    if wiki_name != 'global' and get_wiki_usergroup(user_right, wiki_name) is not None:
        message_to_send = message_to_send.replace("$2", get_wiki_usergroup(user_right, wiki_name))
    else:
        message_to_send = message_to_send.replace("($2)", '')
    message_to_send = message_to_send.replace("$3", expiry_fmt)

    # supporting custom titles by userright
    if local_exists and 'title' in local_data and (user_right in local_data['title'] or 'default' in local_data['title']) and wiki_name != 'global':
        title_to_send = local_data['title'][user_right] if user_right in local_data['title'] else local_data['title']['default']
    else:
        title_to_send = global_data['title']['default']

    if "$1" in title_to_send:
        title_to_send = title_to_send.replace("$1", user_right)
    else:
        # if $2 is not valid, assume it is $1
        title_to_send = title_to_send.replace("$2", get_wiki_usergroup(user_right, wiki_name) if get_wiki_usergroup(user_right, wiki_name) is not None else user_right)
    # and then we can send!
    global only_update_db
 #   print(only_update_db)
    if not only_update_db:
        status = inform_users(wiki_name, user_name, title_to_send, message_to_send)
    else: # we should not send anything
        status = True

    if not status:
        print("Error detected")
        vars.current_stream += 'Error detected \n'
        return  # do not add in database
    # after sending, add its entry in database
    if wiki_name not in local_database:
        local_database[wiki_name] = {}
    if user_expiry not in local_database[wiki_name]:
        local_database[wiki_name][user_expiry] = []

    ll = local_database[wiki_name][user_expiry]
    ll.append([user_name, user_right, user_id])
    local_database[wiki_name][user_expiry] = ll

    # convert that to json and put it back

    user_expiry_database_save(local_database)



def get_opt_out():
    # later on
    ll = get_json_dict('Global_reminder_bot/Exclusion')['targets']
    excluded_users = []
    # print(ll)
    for d in ll:
        #print(d['title'])
        # must be in User namespace - ignore if not
        if 'User:' in d['title']:
            excluded_users.append(re.split('[:/]', d['title'])[1])
    return excluded_users

def user_expiry_database_load():
    # JSON database stored on-wiki
    # [wiki] -> [{expiry_date -> [{user, user_right}]]
    db = get_json_dict('Global_reminder_bot/database')
    return db


def run_approved_wikis():
    # get the list
    ls = get_json_dict('Global_reminder_bot/global')['approved_wikis'] # this is an array
    for wiki_name in ls:
        send_messages(wiki_name)


def user_expiry_database_save(db):
    print(db)
    r = json.dumps(db)
    # save that to db
    wiki_url = 'https://meta.wikimedia.org'
    CSRF_TOKEN, URL, S, api_link = get_token(wiki_url)
    PARAMS_3 = {
        "action": "edit",
        "title": "Global reminder bot/database",
        "contentmodel": "json",
        "bot": "yes",
        "token": CSRF_TOKEN,
        "format": "json",
        "text": r
    }
    R = S.post(URL, data=PARAMS_3)
    # print(R.content)
  #  DATA = R.json()

    # print(DATA)


def send_messages(wiki_name):
    vars.current_stream = ''
    if wiki_name != 'global':
        users = get_users_expiry(wiki_name)
    else:
        users = get_users_expiry_global()

    if users is not None:
        for row in users.itertuples(index=True, name='Pandas'):
            # IMPORTANT: only Leaderbot works on testwiki!
            print(row)
            if ((wiki_name != 'testwiki') or row.username == 'Leaderbot') and 'WMF' not in row.username:
                prepare_message(wiki_name, row.username, row.userright,
                                row.expiry, row.userid)

    vars.central_log[wiki_name] = vars.current_stream


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
    vars.current_stream = vars.current_stream + json.dumps(DATA) + '\n'
    if 'error' in DATA or 'result' not in DATA['edit'] or DATA['edit']['result'] != 'Success':
        return False  # do not proceed - probably ratelimit issue or other failure
    else:
        return True

if __name__ == "__main__":
    input_parser = ap.ArgumentParser(description="Global reminder bot. See [[metawiki:Global reminder bot]]")
    input_parser.add_argument('--only_update_database', type=bool, nargs='?', const=True, default=False,
        help='Does not make any edits to individual edits but updates the database - use only if the database update failed but users were notified')
   # global only_update_db
    args = input_parser.parse_args()
    if args.only_update_database:
        only_update_db = True

    print (f"only_update_db = {only_update_db}")
    try:
        run_approved_wikis()
        wikilist.run_auto_approved_wikis()
    except Exception as e:
        # required since send_central_logging must run
        exception_details = traceback.format_exc()
        print(f"Error detected - details {exception_details}")
        vars.current_stream += f'Error detected {exception_details}\n'
    #print(central_log)
    send_central_logging()