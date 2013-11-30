# -*- coding: utf-8 -*-
'''
    Jakub Gramsz
'''
import MySQLdb as db
from collections import defaultdict
import codecs
import re

def get_synset_id (conn, token, var):
    
    cursor = conn.cursor()    
    sql1 = 'SELECT su.syn_id as synset_id \
           FROM  unitandsynset su \
           JOIN lexicalunit u \
             ON su.lex_id = u.id \
           WHERE u.lemma = "%s" \
             AND u.variant = "%s"' % (token, var)
    cursor.execute(sql1)
    synset_id = cursor.fetchall()[0][0]
    return synset_id

def get_sql_hyperos(synset_id):
    return 'SELECT s.child_id \
		FROM synsetrelation s \
		WHERE s.rel_id =10 \
		AND s.parent_id = "%s"' % synset_id

def get_sql_hipos(synset_id):
    return  'SELECT s.parent_id \
		FROM synsetrelation s \
		WHERE s.rel_id =10 \
		AND s.child_id = "%s"' % synset_id

def get_sql(synset_id):
    return  'SELECT s.child_id \
		FROM synsetrelation s \
		WHERE (s.rel_id =10 OR s.rel_id = 11) \
		AND s.parent_id = "%s"' % synset_id



def distance (token1, var1, token2, var2, max_distance):
    '''
        Funkcja oczekuje na dwa słowa w formie podstawowej i liczy ich odległość w relacji hipo/hyperonimi
        Jeżeli słowa nie są w relacji zwracja -1 (lub max_distance)

	funkcjia potrafi działać bardzo długo dla większych odległości, lub co najgorsze gdy relacja nie zachodzi wcale!
    '''
    conn = db.connect(
     host = "localhost",
     port = 3306,
     user = "root",
     passwd = "firianel",
     db = "plwordnet",
     charset = "utf8",
     use_unicode = True)
    cursor = conn.cursor()  

    synset_id1 = get_synset_id(conn, token1, var1) 
    synset_id2 = get_synset_id(conn, token2, var2)

    #print synset_id1
    #print synset_id2

    dist = 0
    synsetidList = []
    synsetidList.append(synset_id1)
    #print synsetidList
    while not synset_id2 in synsetidList and dist < max_distance:
        if not synsetidList:
	    dist = -1
	    break
    	sqlList = [get_sql(s) for s in synsetidList]
	#print sqlList
	synsetidList = []
	for sqlh in sqlList:
	    cursor.execute(sqlh)
	    synsetidList.extend([item[0] for item in cursor.fetchall()])
	#print synsetidList
	dist += 1

    return dist
