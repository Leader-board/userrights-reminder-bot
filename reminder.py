# global bot reminder
import mysql.connector
import pandas as pd
import requests


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




def get_users_expiry(wiki_name):
    cnx = mysql.connector.connect(option_files='replica.my.cnf', host=f'{wiki_name}.analytics.db.svc.wikimedia.cloud',
                                  database=f'{wiki_name}_p')
    query="""
    SELECT ug.ug_user, u.user_name, ug.ug_group, ug.ug_expiry from user_groups ug
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


def inform_users(wiki_name, user, group, expiry):
    # inform users that their user right will expire soon
    # find the user talk page
    wiki_url = get_url(wiki_name)
    CSRF_TOKEN, URL, S, api_link = get_token(wiki_name)
    # we need to create a new section on that user
    # Step 4: POST request to edit a page
    PARAMS_3 = {
        "action": "edit",
        "title": f"User talk:{user}",
        "section": "new",
        "token": CSRF_TOKEN,
        "format": "json",
        "appendtext": f"Hello {user}! Your {group} right will expire on {expiry}!"
    }
    R = S.post(URL, data=PARAMS_3)
    DATA = R.json()

    print(DATA)





get_users_expiry('enwiki')
get_users_expiry_global()