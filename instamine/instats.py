from datetime import datetime

import mysql.connector

triad_db = mysql.connector.connect(user='instamine',
                                   password='instamine',
                                   host='127.0.0.1',
                                   database='instamine')
c = triad_db.cursor()


def elapsed(start):
    return '({})'.format(str(datetime.now() - start)[:-3])


def pad_percentage(percentage):
    return '{:.2f}%'.format(percentage).rjust(6)


def changes_from_triad(from_type):
    print '\n# {}\n'.format(from_type)

    start = datetime.now()
    c.execute(open('sql/mysql/stats/changes_from.sql').read(), (from_type,))

    stats = {}
    overall_count = 0

    for from_type, to_type, count in c:
        stats[to_type] = count
        overall_count += count

    for to_type, count in stats.iteritems():
        percentage = float(count) / float(overall_count) * 100.0
        print '->{}\t{} ({})'.format(to_type, pad_percentage(percentage), count)

    print '\n({})'.format(overall_count), elapsed(start)


def changes_from():
    uber_start = datetime.now()

    c.execute('SELECT name FROM triad_type')

    for triad_type_name, in c.fetchall():
        changes_from_triad(triad_type_name)

    print '\n' + elapsed(uber_start)


changes_from()
triad_db.close()
