# -*- coding: utf-8 -*-
"""
    Jakub Gramsz
"""

import MySQLdb as db
from collections import defaultdict
import codecs
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
        Jeżeli słowa nie są w relacji zwracja -1, wprzypadku gdy zabraknie następników.
    """
    conn = get_connection()
    cursor = conn.cursor()

    synset_ids1 = get_synset_ids(conn, token1)
    print( synset_ids1)
    synset_ids2 = get_synset_ids(conn, token2)
    print( synset_ids2)

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

        print("Dist: {0}, Liczba następników: hyper {1}, hipo {2}".format(dist + 1,
                                                                len(synsetidList_hyperos),
                                                                len(synsetidList_hipos)))

    return dist
