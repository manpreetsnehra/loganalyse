#!/usr/bin/python
import geoip2.database
import geoip2.webservice
import argparse
import sqlite3
import sys
import re
import socket

#Try to resolve the hostname if fails return unresolvable
def resolve(hostname):
	try: 
		ip = socket.gethostbyname(hostname)
	except:
		ip = "unresolvable"
	return ip		

#Process Command ine arguments	
parser = argparse.ArgumentParser(description="Use GeoIP database to pinpoint location of users on a map")
source = parser.add_mutually_exclusive_group(required=True)
source.add_argument('--datasource', help='Use local Datalocation',choices=['web','local'])
source.add_argument('--droptable',help="remove existing ip to geo table",choices=['1','0'])
args = parser.parse_args()

#open the db
try: 
    conn = sqlite3.connect('accesslogs.sqlite')
except:
    print "Logs database file not found"
    sys.exit(1)
    
cur=conn.cursor()
#Create location table for location info 
cur.execute('''CREATE TABLE IF NOT EXISTS Location ( ip TEXT PRIMARY KEY UNIQUE,country TEXT,city TEXT, latitude FLOAT,longitude FLOAT,frequency INTEGER)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Unresolvable (hostname TEXT PRIMARY KEY UNIQUE)''')

#drop the table and clear processed logs for redoing
if args.droptable == '1':
    cur.execute('''DROP TABLE IF EXISTS Location''')
    cur.execute('''UPDATE Logsprocessed set location=0''')
    conn.commit()
    sys.exit(1)

#reference Docs http://geoip2.readthedocs.org/en/latest/ if you are not using a downloaded version of the db then the corresponding webservice can be used
if args.datasource == 'local':
    try:
        reader = geoip2.database.Reader('/var/lib/GeoIP/GeoLite2-City.mmdb')
    except:
        print "Database file does not exist"
        sys.exit(1)
elif args.datasource == 'web':
    try:
        reader = geoip2.webservice.Client(42, 'license_key')
    except:
        print "Could not reach geoip2 webservice"        
        sys.exit(1)

#process 10000 log entries before each commit back to db    
while True:
    cur.execute('''SELECT Logs.id,Logs.remote_user from Logs JOIN LogsProcessed ON Logs.id = LogsProcessed.id where Logsprocessed.location = 0 LIMIT 10000''')
    try:
        records=cur.fetchall()
    except:
        break
    for remote in records:
    	#only try to resolve if not an IP
    	if re.search(r"[a-zA-Z]+",remote[1]) is not None:	
    		ip = resolve(remote[1])
    	else:
    		ip = remote[1]
    	print remote[0],remote[1],ip
        try:
            user = reader.city(remote[1])
        except:
            cur.execute('''UPDATE LogsProcessed set location = 1 where id=?''',(remote[0],))                   
            continue
    	if ip == 'unresolvable':
    		cur.execute('''INSERT OR IGNORE INTO Unresolvable (hostname) values (?)''',(ip,))
    		conn.commit()
    		continue        
        cur.execute('''SELECT frequency from location where ip = ?''',(remote[1],))
        frequency_info=cur.fetchone()        
        if frequency_info == None:
            cur.execute('''INSERT OR IGNORE INTO Location (ip,frequency) VALUES (?,?)''',(ip,1,))
        else:
            frequency = frequency_info[0]
            frequency = frequency+1
            cur.execute('''UPDATE Location set frequency = ? where ip = ?''',(frequency,ip,))
        data=(user.country.name,user.city.name,user.location.latitude,user.location.longitude,ip,)
        cur.execute('''UPDATE Location set country=?,city=?,latitude=?,longitude=? where ip=?''',data)
        cur.execute('''UPDATE LogsProcessed set location = 1 where id=?''',(remote[0],))                   
    conn.commit()
