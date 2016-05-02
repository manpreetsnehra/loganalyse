#!/usr/bin/python

import urllib
from BeautifulSoup import *
from urlparse import urlparse
from urlparse import urljoin
import argparse
import os
import sqlite3
import re
import string
import sys

def read(data):
    logfile = open(data)
    for line in logfile:
        try:
            record = re.search('([\d+\.]+) - - \[(.*?)\] "(.*?)" (\d+) (.*?) "(.*?)" "(.*?)"',line).groups()
            (datetime,zone) = string.split(record[1])
            (command,request,responseproto) = string.split(record[2])
            (date,time)=string.split(datetime,":",1)
        except:
            continue
        values=(record[0],date,time,int(zone),command,request,responseproto,int(record[3]),record[4],record[5],record[6],)
        logsrecord=re.match("^/logs/(.*)",request)
        if logsrecord == None:        
            cur.execute('''INSERT OR IGNORE INTO Logs (remote_user,date,time,timezone,command,request,responseproto,status,bytes_sent,referer,ua,processed) VALUES(?,?,?,?,?,?,?,?,?,?,?,0)''',values )
    logfile.close()

def downloadlogs(url):
    if not os.path.exists("data"):
        os.makedirs("data")
    index=urllib.urlopen(url)
    filelist=index.read()
    soup=BeautifulSoup(filelist)
    tags = soup('a')
    for tag in tags:
        href = tag.get('href', None)
        if href.endswith('log') :
            loguri=urljoin(url,href)
            logfile=string.join(["data",href],"/")            
            urllib.urlretrieve(loguri,logfile)
            cur.execute('''INSERT OR IGNORE INTO Logfiles (fileuri,status) VALUES (?,)''',(href,)) 
                                        
conn = sqlite3.connect('accesslogs.sqlite')
cur = conn.cursor()
#create table for all data from files to SQL
cur.execute('''CREATE TABLE IF NOT EXISTS Logs (id INTEGER PRIMARY KEY AUTOINCREMENT, remote_user TEXT , date TEXT,time TEXT, timezone INTEGER, command TEXT, request TEXT, responseproto TEXT,status INTEGER, bytes_sent TEXT, referer TEXT,ua TEXT,processed INTEGER)''')
cur.execute('''CREATE TABLE IF NOT EXISTS LogFiles (id INTEGER PRIMARY KEY AUTOINCREMENT, fileuri TEXT, status INTEGER)''')
                
parser = argparse.ArgumentParser(description="Read Apache Logs and push into sqlite")
source = parser.add_mutually_exclusive_group(required=True)
source.add_argument('--data', help='Data Directory location',type=str)
source.add_argument('--url', help='URL Location',type=str)
source.add_argument('--resetLogs',help='Set All Logs to be unprocessed',choices=['0','1'])
args = parser.parse_args()   


downloadlogs(args.url)
    
if args.resetLogs == "1":
    cur.execute('''UPDATE Logs set processed=0''')
    conn.commit()
    sys.exit(1)    
    
#check if the data entered is a file or dir
if os.path.isfile(args.data):
    read(args.data)
elif os.path.isdir(args.data):
    listing=os.listdir(args.data)
    for logfile in listing:
        cur.execute('''SELECT status from LogFiles where fileuri=? ''',(logfile,))
        done=cur.fetchone()
        if  done == None or ( done != None and done[0] != 1):
            cur.execute('''INSERT OR IGNORE INTO LogFiles (fileuri,status) VALUES (?,?) ''',(logfile,0,))
            log = string.join([args.data,logfile],"/")
            read(log)
            cur.execute('''UPDATE LogFiles set status = 1 where fileuri=? ''',(logfile,))
        conn.commit()
cur.close()           