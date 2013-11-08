# -*- coding: utf-8 -*-
'''
Jan Kocoń
Politechnika Wrocławska
jan.kocon@pwr.wroc.pl
'''
import networkx as nx
import MySQLdb as db
from collections import defaultdict
import codecs
import re

'''
* Estratto list:
  hyponym - hyperonym
* Main relation from WordNet
  A is hyponym B
  relationtype.id=10
'''

'''
#Sample NetworkX usage
import matplotlib.pyplot as plt
G=nx.DiGraph()
G.add_edge("zamek","budowla")
G.add_edge("budowla","obiekt")
G.add_edge("a","b")
print "zamek -> budowla %s" % nx.has_path(G,"zamek","budowla")
print "budowla -> obiekt %s" % nx.has_path(G,"budowla","obiekt")
print "zamek -> obiekt %s" % nx.has_path(G,"zamek","obiekt")
print "obiekt -> zamek %s" % nx.has_path(G,"obiekt","zamek")
print "obiekt -> budowla %s" % nx.has_path(G,"obiekt","budowla")
print "a -> b %s" % nx.has_path(G,"a","b")
print "b -> a %s" % nx.has_path(G,"b","a")
nx.draw(G)
plt.show()
'''


def make_wordnet_list():
    conn = db.connect(
              host = "localhost",
              port = 3306,
              user = "root",
              passwd = "alamakota",
              db = "wordnet_29_08_2012",
              charset = "utf8",
              use_unicode = True)
    cursor = conn.cursor()    
    sql = 'SELECT u.lemma as hyponym, u.variant as v1, \
                  u2.lemma as hyperonym, u2.variant as v2 \
           FROM synsetrelation s \
           JOIN unitandsynset su \
             ON s.parent_id = su.syn_id \
           JOIN unitandsynset su2 \
             ON s.child_id = su2.syn_id \
           JOIN lexicalunit u \
             ON su.lex_id = u.id \
           JOIN lexicalunit u2 \
             ON su2.lex_id = u2.id \
           WHERE s.rel_id = 10 \
             AND u.lemma not like "%% %%" \
             AND u2.lemma not like "%% %%" \
           ORDER BY u2.lemma'
    cursor.execute(sql)
    wordnetList = cursor.fetchall()
    f = codecs.open('wordnet_list_2.txt','w','utf-8')
    for (hyponym, v1, hyperonym, v2) in wordnetList:
        f.write("%s;%s;%s;%s\n" % (hyponym,v1,hyperonym,v2))
    f.close()
 
def make_estratto_list():
    def get_normalized(entity):
        if ":" in entity:
            return entity.split(":")[1]
        else: 
            return entity
        
    p = re.compile('<pt>\[base="(.*)"\]</pt> <pt>\[base="(.*)"\]</pt>')    
    f = codecs.open('instancje.txt4','r','utf-8')    
    f2 = codecs.open('estratto_list.txt','w','utf-8')
    for line in f:
        m = p.search(line)
        if m and not "][" in line:
            (hyponym, hyperonym) = (get_normalized(m.group(1)), 
                                    get_normalized(m.group(2)))
            f2.write("%s;%s\n" % (hyponym,hyperonym))
    f.close()
    f2.close()
        
def make_estratto_extended_list():
    '''
    Creates directed graph from WordNet one word lemmas connected with hyponymy 
    relation and checks if exists the path between two lemmas from each instance 
    of Estratto hyponymy relation list. 
    '''

    wordnetGraph = nx.DiGraph()
    wordnetDict = defaultdict(set)
    f = codecs.open('wordnet_list_2.txt','r','utf-8')
    for line in f:
        (hyponym, v1, hypernym, v2) = line[:-1].split(";")
        wordnetGraph.add_edge("%s-%s" % (hyponym,  v1), 
                              "%s-%s" % (hypernym, v2))
        wordnetDict[hyponym].add(v1)
        wordnetDict[hypernym].add(v2)
    f.close()
    
    estrattoPairs = set()
    ep = set()
    ep2 = set()
    f = codecs.open('estratto_list.txt','r','utf-8')
    for line in f:
        (hyponym, hypernym) = line[:-1].split(";")
        ep.add((hyponym, hypernym))
        if (hyponym in wordnetDict and
           hypernym in wordnetDict):
           instanceNotExists = True
           for v1 in wordnetDict[hyponym]:
               for v2 in wordnetDict[hypernym]:
                   hypoV = "%s-%s" % (hyponym,  v1)
                   hyperV = "%s-%s" % (hypernym, v2)
                   if (hypoV != hyperV and 
                       nx.has_path(wordnetGraph, hypoV, hyperV)):
                       estrattoPairs.add((1, (hyponym, v1, hypernym, v2)))
                       instanceNotExists = False
           if instanceNotExists:
               estrattoPairs.add((0, (hyponym, hypernym)))
        else:
            ep2.add((hyponym, hypernym))
            estrattoPairs.add((0, (hyponym, hypernym)))
    f.close()
    
    f = codecs.open('estratto_extended_list_2.txt','w','utf-8')
    estrattoInstancesAll = 0
    estrattoInstancesWordnet = 0
    for (wnExists, item) in estrattoPairs:
        estrattoInstancesAll += 1
        if wnExists:
            (hyponym, v1, hypernym, v2) = item
            estrattoInstancesWordnet += 1            
            f.write("%s;%s;%s;%s;%s\n" % (wnExists, hyponym, v1, hypernym, v2))
        else:
            (hyponym, hypernym) = item
            f.write("%s;%s;%s\n" % (wnExists, hyponym, hypernym))
        
    f.close()
    print "All instances: %s" % estrattoInstancesAll
    print "Instances in WordNet: %s" % estrattoInstancesWordnet
    print "EP: %s" % len(ep)
    print "EP2: %s" % len(ep2)    
    
#make_wordnet_list()    
#make_estratto_list()
make_estratto_extended_list()




'''
before:
All instances: 784735
Instances in WordNet: 275604
after:
All instances: 274611
Instances in WordNet: 13706
after2:
All instances: 787041                                                                                               
Instances in WordNet: 13706 
after3:
All instances: 786899
Instances in WordNet: 13516
'''