# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
#!/usr/bin/python
# -*- coding: utf-8 -*-

# universities.txt : contains universities along with their ranking picked from QS ranking text file
# results.txt : contains actual university name and the matched affiliations for the actual name

import MySQLdb as mdb
import sys

f = open('tempresults.txt', 'w')
fresults = open('results.txt', 'w')
con = mdb.connect('155.69.120.236', 'msa_usr', 'msa_pwd', 'microsoftacademicgraph');
cur = con.cursor()
try:
    for line in open("universities.txt"):
        university = line.split('\t')[1].strip().lower()
        actual2shingles = [university[i:i+2] for i in range(len(university)-2+1)]
        dict2shingles = {}        
        for shingle in actual2shingles:
            if shingle not in dict2shingles:
                dict2shingles[shingle] = 1
                
        actual3shingles = [university[i:i+3] for i in range(len(university)-3+1)]
        dict3shingles = {}        
        for shingle in actual3shingles:
            if shingle not in dict3shingles:
                dict3shingles[shingle] = 1

        actual4shingles = [university[i:i+4] for i in range(len(university)-4+1)]
        dict4shingles = {}        
        for shingle in actual4shingles:
            if shingle not in dict4shingles:
                dict4shingles[shingle] = 1
    
        words = university.split(' ')
        pattern = ""

        for word in words:
            pattern = pattern + "%" + word[0:3]
        pattern = pattern + "%"
        f.write('Pattern chosen : %s\n' %pattern)
        # Run the following statement to filter some of the matches from the big database
        # stmt = "select count(affiliation_name) from affiliations where affiliation_name like %s and affiliation_name not in (select affiliation_name from affiliations where affiliation_name like %s"
        stmt = "select affiliation_name from affiliations where LOWER(affiliation_name) like '%s'"%(pattern)
        print stmt
        cur.execute(stmt)
        # results = cur.fetchall()
        row = cur.fetchone()
        count = 0
        while row is not None:
            match = row[0]
            f.write("\nAffiliation : %s\n" %match)
            aff2shingles = [match[i:i+2] for i in range(len(match)-2+1)]
            aff3shingles = [match[i:i+3] for i in range(len(match)-3+1)]
            aff4shingles = [match[i:i+4] for i in range(len(match)-4+1)]

            match2shingles = {}
            match3shingles = {}
            match4shingles = {}

            for shingle in dict2shingles:
                if shingle in aff2shingles:
                    if shingle not in match2shingles:
                        match2shingles[shingle] = 1
                else:
                    match2shingles[shingle] = 0
    
            for shingle in dict3shingles:
                if shingle in aff3shingles:
                    if shingle not in match3shingles:
                        match3shingles[shingle] = 1
                else:
                    match3shingles[shingle] = 0
            
            for shingle in aff4shingles:
                if shingle in dict4shingles:                
                    if shingle not in match4shingles:
                        match4shingles[shingle] = 1
                else:
                    match4shingles[shingle] = 0

#            f.writelines('{}:{} '.format(k,v) for k, v in match3shingles.items())
#            f.write('\n')
#            f.writelines('{}:{} '.format(k,v) for k, v in dict3shingles.items())       

#            f.write("\nsum(match2shingles) : %s sum(dict2shingles) : %s; proportion(2shingles) : %s \n" % (sum(match2shingles.values()) , sum(dict2shingles.values()) , str(sum(match2shingles.values())/float(sum(dict2shingles.values())))))
#            f.write("\nsum(match3shingles) : %s; sum(dict3shingles) : %s; proportion(3shingles) : %s \n" % ( sum(match3shingles.values()) , sum(dict3shingles.values()) , str(sum(match3shingles.values())/float(sum(dict3shingles.values())))))
#            f.write("\nsum(match4shingles) : %s; sum(dict4shingles) : %s; proportion(4shingles) : %s \n" % (sum(match4shingles.values()) , sum(dict4shingles.values()) , str(sum(match4shingles.values())/float(sum(dict4shingles.values())))))
            shingle2 = sum(match2shingles.values())/float(sum(dict2shingles.values()))
            shingle3 = sum(match3shingles.values())/float(sum(dict3shingles.values()))
            shingle4 = sum(match4shingles.values())/float(sum(dict4shingles.values()))            
            f.write("\n2-shingles:"+str(shingle2)+" 3-shingles:"+ str(shingle3)+"4-shingles:"+str(shingle4)+"\n")
            if shingle2 > 0.8 and shingle3 > 0.7 and shingle4 > 0.65:
                fresults.write(count +"\t"+match+"\n\n")
                count = count + 1
            row = cur.fetchone()            
        #cur.prepare("select count(affiliation_name) from affiliations where affiliation_name like $1 and affiliation_name not in (select affiliation_name from affiliations where affiliation_name like $2)")
        #cur.execute("%stan%univ%", "%stanford university%")
except mdb.Error, e:
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)
finally:
    if con:
        con.close()
f.close()
fresults.write("\nNumber of matches:"+str(count))
print "\nnumber of matches :"+count
fresults.close()