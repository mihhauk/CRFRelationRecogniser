# -*- coding: utf-8 -*-
"""Usage:
    dist_tls.py test <file>
    dist_tls.py distance <token1> <token2> [<max_distance>]

Author: Jakub A. Gramsz
"""

import MySQLdb as db
import codecs
from docopt import docopt
import re


def get_connection():
    return db.connect(
                        host="localhost",
                        port=3306,
                        user="root",
                        passwd="firianel",
                        db="plwordnet",
                        charset="utf8",
                        use_unicode=True)


def get_synset_ids(conn, token):
    cursor = conn.cursor()
    sql1 = 'SELECT su.syn_id as synset_id \
           FROM  unitandsynset su \
           JOIN lexicalunit u \
             ON su.lex_id = u.id \
           WHERE u.lemma = "%s" ' % token
    cursor.execute(sql1)
    synset_ids = [item[0] for item in cursor.fetchall()]
    return synset_ids


def get_sql_hyperos(synset_id):
    return 'SELECT s.child_id \
            FROM synsetrelation s \
            WHERE s.rel_id =10 \
            AND s.parent_id = "%s"' % synset_id


def get_sql_hipos(synset_id):
    return 'SELECT s.parent_id \
            FROM synsetrelation s \
            WHERE s.rel_id =10 \
            AND s.child_id = "%s"' % synset_id


def distance(token1, token2, max_distance=100):
    """
        Funkcja oczekuje na dwa słowa w formie podstawowej i liczy ich odległość w relacji hipo-/hyperonimi
        Jeżeli słowa nie są w relacji zwracja -1, wprzypadku gdy zabraknie następników. (zmienię na ang.)
    """
    conn = get_connection()
    cursor = conn.cursor()

    synset_ids1 = get_synset_ids(conn, token1)
    # print( synset_ids1)
    synset_ids2 = get_synset_ids(conn, token2)
    # print( synset_ids2)

    dist = 0
    synsetidList_hyperos = synset_ids1
    synsetidList_hipos = synset_ids1

    while not set(synset_ids2).intersection(set(synsetidList_hyperos)) \
          and not set(synset_ids2).intersection(set(synsetidList_hipos)) \
          and dist < max_distance:
        if not synsetidList_hyperos and not synsetidList_hipos:
            dist = -1
            break
        sqlList_hyperos = [get_sql_hyperos(s) for s in synsetidList_hyperos]
        sqlList_hipos = [get_sql_hipos(s) for s in synsetidList_hipos]
        synsetidList_hyperos = []
        synsetidList_hipos = []
        for sqlh in sqlList_hyperos:
            cursor.execute(sqlh)
            synsetidList_hyperos.extend([item[0] for item in cursor.fetchall()])
        for sqlh in sqlList_hipos:
            cursor.execute(sqlh)
            synsetidList_hipos.extend([item[0] for item in cursor.fetchall()])
        dist += 1

        #print("Dist: {0}, Liczba następników: hyper {1}, hipo {2}".format(dist + 1,
        #                                                        len(synsetidList_hyperos),
        #                                                        len(synsetidList_hipos)))

    return dist


def test_dist(file_name):
    f = codecs.open('wordnet_explorer/wordnet_list_2.txt','r','utf-8')
    f2 = codecs.open(file_name, 'w', 'utf-8')
    for line in f:
        (hyponym, v1, hypernym, v2) = line[:-1].split(";")
        f2.write("%s;%s;%s;%s;%s\n" % (hyponym, v1, hypernym, v2, distance(hyponym, hypernym, 1000)) )
    f2.close()
    f.close()


arguments = docopt(__doc__)

if arguments['test']:
    test_dist(arguments['<file>'])
if arguments['distance']:
    if arguments['<max_distance>']:
        print("Distance between tokens is %s" % (distance(arguments['<token1>'],
                                                 arguments['<token2>'],
                                                 arguments['<max_distance>'])) )
    else:
        print("Distance between tokens is %s" % distance(arguments['<token1>'], arguments['<token2>']))