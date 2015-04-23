import os
import sqlite3

DB_ORIGIN = '../data/number_ten.db'
DB_COPY = '../data/number_ten (201 rescue followings).db'

try:
    os.remove(DB_COPY)
except OSError:
    pass

origin_conn = sqlite3.connect(DB_ORIGIN)
origin = origin_conn.cursor()
copy_conn = sqlite3.connect(DB_COPY)
copy = copy_conn.cursor()

origin.execute('SELECT COUNT(*) FROM following WHERE first_seen > (SELECT fin FROM effort WHERE id = 1);')
print origin.fetchone()[0], 'new triads'

copy.execute('CREATE TABLE following ('
             'follower_id INTEGER,'
             'followee_id INTEGER,'
             'PRIMARY KEY (follower_id, followee_id))')

origin.execute('SELECT follower_id, followee_id FROM following '
               'WHERE first_seen > (SELECT fin FROM effort WHERE id = 1)')
for res in origin:
    copy.execute('INSERT INTO following (follower_id, followee_id) VALUES (?, ?)', res)

copy_conn.commit()
