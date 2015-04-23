import mysql.connector
import os
import sqlite3

DB_ORIGIN = '../data/number_ten.db'
DB_COPY = '../data/number_ten (new followings copy).db'

origin_conn = sqlite3.connect(DB_ORIGIN)
origin = origin_conn.cursor()
copy_conn = sqlite3.connect(DB_COPY)
copy = copy_conn.cursor()
mysql_con = mysql.connector.connect(user='instamine',
                                    password='instamine',
                                    host='localhost',
                                    database='instamine')
mdb = mysql_con.cursor()

# mdb.execute('SELECT * FROM triad')
#
# for row in mdb:
# print row
#
# triad_type = '201'
# mdb.execute('SELECT id FROM triad_type WHERE name = %s', (triad_type,))
# print mdb.fetchone()[0]

# mdb.execute('SELECT id FROM triad_type WHERE name = %s', ('201',))
# triad_type_id = mdb.fetchone()[0]
# print triad_type_id
#
# mdb.executemany('INSERT IGNORE INTO triad (a_id, b_id, c_id, triad_type_id) VALUES (%s, %s, %s, %s)',
# [(1, 2, 3, 1), (1, 2, 3, 2)])
# mysql_con.commit()

# triad_types = [x[:-4] for x in os.listdir('sql/triads')]
# for triad_type in triad_types:
# mdb.execute('INSERT INTO triad_type (name) VALUES (%s)', (triad_type,))
#
# mysql_con.commit()

# origin.execute('SELECT COUNT(*) FROM following WHERE first_seen > (SELECT fin FROM effort WHERE id = 1)')
# print origin.fetchone()[0], 'new triads'
#
# copy.execute('CREATE TABLE following ('
# 'follower_id INTEGER,'
#              'followee_id INTEGER,'
#              'PRIMARY KEY (follower_id, followee_id))')
#
# origin.execute('SELECT follower_id, followee_id FROM following '
#                'WHERE first_seen > (SELECT fin FROM effort WHERE id = 1)')
# for res in origin:
#     copy.execute('INSERT INTO following (follower_id, followee_id) VALUES (?, ?)', res)
#
# copy_conn.commit()

# copy.execute('SELECT follower_id, followee_id FROM following')
# origin.executemany("INSERT INTO following (follower_id, followee_id, first_seen) VALUES (?, ?, datetime('now'))",
#                    copy.fetchall())
#
# origin_conn.commit()
#

# mdb.execute("SELECT * FROM triad "
#             "WHERE first_seen < (SELECT MAX(fin) FROM effort) "
#             "AND a_id IN (%s, %s, %s) "
#             "AND b_id IN (%s, %s, %s) AND c_id IN (%s, %s, %s)",
mdb.execute("SELECT a_id, b_id, c_id FROM triad WHERE first_seen > (SELECT MAX(fin) FROM effort)")
new_triads = mdb.fetchall()

print 'new triads:', len(new_triads)

for a_id, b_id, c_id in new_triads:
    mdb.execute(open('sql/mysql/change.sql').read(),
                (a_id, b_id, c_id,
                 a_id, b_id, c_id,
                 a_id, b_id, c_id))
    prev_triads = mdb.fetchall()
    if len(prev_triads) > 0:
        print len(prev_triads)
