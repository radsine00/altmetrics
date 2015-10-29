# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 16:18:28 2015

@author: rads

"""
#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb as mdb
import sys

f = open('results.txt', 'w')

con = mdb.connect('155.69.120.236', 'msa_usr', 'msa_pwd', 'microsoftacademicgraph');
cur = con.cursor()
con1 = mdb.connect('155.69.120.236', 'msa_usr', 'msa_pwd', 'microsoftacademicgraph');
cur1 = con1.cursor()
con2 = mdb.connect('155.69.120.236', 'msa_usr', 'msa_pwd', 'microsoftacademicgraph');
cur2 = con2.cursor()
con3 = mdb.connect('155.69.120.236', 'msa_usr', 'msa_pwd', 'microsoftacademicgraph');
cur3 = con3.cursor()
con4 = mdb.connect('155.69.120.236', 'msa_usr', 'msa_pwd', 'microsoftacademicgraph');
cur4 = con4.cursor()

try:
    cur.execute("SELECT VERSION()")
    ver = cur.fetchone()
    f.write("Database version : %s \n" % ver)
    nodes = set()
    adjlist = {}
    for journal in open("journal.txt"):
        journal = journal.strip()
        stmt = "select journal_id from journals where LOWER(journal_name) like '%s'"%(journal)
        print stmt
        cur.execute(stmt)
        # results = cur.fetchall()
        row = cur.fetchone()
        while row is not None:
            journal_id = row[0]
            stmt1 = "select paper_id from papers where LOWER(journal_id) like '%s' limit 100"%(journal_id)
            cur1.execute(stmt1)
            row1 = cur1.fetchone()
            while row1 is not None:
                paper_id = row1[0]
                print paper_id
                stmt2 = "select author_id, affiliation_id from paperauthoraffiliations where paper_id like '%s' limit 100" %(paper_id)
                cur2.execute(stmt2)
                row2 = cur2.fetchone()
                while row2 is not None:
                    author_id = row2[0]
                    affiliation_id = row2[1]
                    if affiliation_id == '':
                        row2 = cur2.fetchone()
                        continue
                    node = tuple()
                    node = (author_id, affiliation_id)
                    nodes.add(node)
                    s = 'node[0] and node[1] are '+node[0] +' and '+node[1]+'\n'
                    print s
                    node1 = node
                    s1 = 'node1[0] and node1[1] are '+node1[0] +' and '+node1[1]+'\n'
                    print s1
                    
                    adjlist[node1] = set()
                    stmt3 = "select reference_id from paperreferences where paper_id like '%s' limit 100"%(paper_id)
                    cur3.execute(stmt3)
                    row3 = cur3.fetchone()
                    while row3 is not None:
                        reference_id = row3[0]
                        stmt4 = "select author_id, affiliation_id from paperauthoraffiliations where paper_id = '%s' limit 100" %(reference_id)
                        cur4.execute(stmt4)
                        row4 = cur4.fetchone()
                        while row4 is not None:
                            author_id = row4[0]
                            affiliation_id = row4[1]
                            if affiliation_id == '':
                                row4 = cur4.fetchone()
                                continue
                            
                            node = tuple()
                            node = (author_id, affiliation_id)
                            nodes.add(node)
                            
                            node2 = node
                           # print node2[0], node2[1]
                            adjlist[node1].add(node2)
                            
                            row4 = cur4.fetchone()
                        row3 = cur3.fetchone()
                    row2 = cur2.fetchone()
                row1 = cur1.fetchone()
            row = cur.fetchone()

except mdb.Error, e:
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)
finally:
    if con:
        con.close()
    if con1:
        con1.close()
    if con2:
        con2.close()
    if con3:        
        con3.close()
    if con4:        
        con4.close()
for k, v in adjlist.items():
    print k, v
f.writelines('{}:{} \n'.format(k,v) for k, v in adjlist.items())
f.write('Hello')
f.close()