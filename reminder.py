# global bot reminder
import mysql.connector
import pandas as pd

def get_users_expiry(wiki_name):
    cnx = mysql.connector.connect(option_files='$HOME/replica.my.cnf', host=f'{wiki_name}.analytics.db.svc.wikimedia.cloud',
                                  database='meta_p')
    query="""
    SELECT ug.ug_user, u.user_name, ug.ug_group, ug.ug_expiry from user_groups ug
    INNER JOIN user u
    ON u.user_id = ug.ug_user
    WHERE ug_expiry is not null
    AND ug_expiry < NOW() + INTERVAL 2 WEEK
    """
    cursor = cnx.cursor()
    cursor.execute(query)
    res = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    print(res)
    cursor.close()

get_users_expiry('meta_wiki')