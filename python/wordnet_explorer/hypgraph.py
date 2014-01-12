#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Author: Jakub A. Gramsz

Usage:
    hypgraph.py make <filename> [-l <limit>] [-s <start_from>]
    hypgraph.py list -i <graph_filename> -d <num_distance> [-o <filename>]

Options:
    -i <graph_filename>, --input <graph_filename>
    -d <num_distance>, --distance <num_distance> [default: 1]
    -o <filename>, --output <filename>
    -l <limit>, --limit <limit>
    -s <start_from>, --start-from <start_from>
"""

import MySQLdb as db
import codecs
from docopt import docopt
from graph_tool.all import *


def get_connection():
    return db.connect(
                        host="localhost",
                        port=3306,
                        user="root",
                        passwd="firianel",
                        db="plwordnet",
                        charset="utf8",
                        use_unicode=True)


def get_sql_limit(a, b):
    return 'SELECT u.lemma as hyponym, u.variant as v1, \
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
     AND u.pos = 2\
         AND u2.pos = 2\
       ORDER BY u2.lemma\
        LIMIT %s, %s' % (a, b)

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
     AND u.pos = 2\
         AND u2.pos = 2\
       ORDER BY u2.lemma'


def get_vertices_such(graph, lex, var):
    matched_lex = find_vertex(graph, graph.vp["lex"], lex.encode('UTF-8', 'replace'))
    for m in matched_lex:
        if not graph.vp["var"][m] == var:
            matched_lex.remove(m)
    return matched_lex

def add_or_get_vertex(g,vps, lex, var):
    (vprop_lex, vprop_var) = vps
    matched = get_vertices_such(g, lex, var)
    if len(matched) == 0:
        v = g.add_vertex()
        vprop_lex[v] = lex.encode('UTF-8', 'replace')
        vprop_var[v] = var
        #print("  Tworzę nowy wieżchołek")
    else:
        v = matched[0]
        if len(matched) == 1:
            #print("  Wieżchołek już istniał")
            pass
        else:
            print ("  Istenie więcej niż jeden (", lex, ", ", var, ")")
    return v


def make_graph2(sql_source):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql_source)
    wordnetList = cursor.fetchall()
    tot = len(wordnetList)
    t = 0
    #print 'Do dodania do grafu %d krawędzi' % tot
    g= Graph(directed=False)
    vprop_lex = g.new_vertex_property("string")
    vprop_var = g.new_vertex_property("int")
    vps = (vprop_lex, vprop_var)
    g.vp['lex'] = vprop_lex
    g.vp['var'] = vprop_var
    for (hyponym, v1, hyperonym, v2) in wordnetList:
        #print hyponym, v1, hyperonym, v2
        ver1 = add_or_get_vertex(g, vps, hyponym, v1)
        ver2 = add_or_get_vertex(g, vps, hyperonym, v2)
        g.add_edge(ver1 , ver2)
        #t+=1
        #if t % 100 == 0:
        #print 'Dodano %d / %d' % (t, tot)
    return g


def set_of_paths(steps, g):
    if steps < 1:
        return set([])
    elif steps == 1:
        return set([(e.source(), e.target()) for e in g.edges()])
    else:
        return join(set_of_paths(1, g), set_of_paths(steps-1,g))


def join (set1, set2):
    news = set()
    for ed1 in set1:
        for ed2 in set2:
            if ed1[1] == ed2[0]:
                news.add( (ed1[0], ed2[1]) )
    return news


def svp(g, vertex):
    prop_lex = g.vertex_properties["lex"]
    prop_var = g.vertex_properties["var"]
    return ";".join(( prop_lex[vertex].decode('UTF-8'), str(prop_var[vertex]) ))


def test_graph():
    g = Graph()
    g.add_vertex(10)
    g.add_edge(g.vertex(0),g.vertex(1))
    g.add_edge(g.vertex(1),g.vertex(2))
    g.add_edge(g.vertex(1),g.vertex(3))
    g.add_edge(g.vertex(1),g.vertex(4))
    g.add_edge(g.vertex(1),g.vertex(5))
    g.add_edge(g.vertex(6),g.vertex(7))
    g.add_edge(g.vertex(7),g.vertex(8))
    g.add_edge(g.vertex(3),g.vertex(9))
    return g


def make_file_list(graph_file_name, file_name, dist):
    f = codecs.open(file_name, 'w', 'utf-8')
    graph = Graph()
    graph.load(graph_file_name)
    for (s,t) in set_of_paths(dist, graph):
        f.write( '%s;%s\n' % (svp(graph, s), svp(graph, t)) )
    f.close()


def print_list(graph_file_name, dist):
    graph = Graph()
    graph.load(graph_file_name)
    dist = dist
    for (s,t) in set_of_paths(dist, graph):
        print svp(graph, s),";",svp(graph, t)


def run():
    arguments = docopt(__doc__)

    if arguments['make']:
        if bool(arguments['--limit']) and bool(arguments['--start-from']):
            start_from = int(arguments['--start-from'])
            limit = int(arguments['--limit'])
            graph = make_graph2(get_sql_limit(start_from, limit))
            graph.save(arguments['<filename>'])
        else:
            graph = make_graph2(sql)
            graph.save(arguments['<filename>'])
    elif arguments['list']:
        if arguments['--output']:
            make_file_list(arguments['--input'],
                           arguments['--output'],
                           int(arguments['--distance']))
        else:
            print_list(arguments['--input'],
                       int(arguments['--distance']))


run()
