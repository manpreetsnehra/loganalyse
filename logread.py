#!/usr/bin/python
###################################################################
### Read the logs 
###################################################################
import urllib
from BeautifulSoup import BeautifulSoup
from urlparse import urljoin
import argparse
import os
import sqlite3
import re
import string
import sys
import socket

#read the apache log files line by line
def read(data):
    logfile = open(data)
#setup dict to resolve month name to number    
    month_dict = {"Jan":"01","Feb":"02","Mar":"03","Apr":"04", "May":"05", "Jun":"06","Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"}
         #iterate over the log file one line at a time
    for line in logfile:
        try:
        	#break the line as a regex into various parts
            record = re.search('(.*?) - - \[(.*?)\] "(.*?)" (\d+) (\d+)',line).groups() or re.search('([\d+\.]+) - - \[(.*?)\] "(.*?)" (\d+) (.*?) "(.*?)" "(.*?)"',line).groups()
            #split the datetime and timezone
            (datetime,zone) = string.split(record[1])
            #further extracting for command sent the URL request and the protocol used
            (command,request,responseproto) = string.split(record[2])
            #split datetime into date and time
            (date,time)=string.split(datetime,":",1)
            #split date into day month year
            (day,month,year)=string.split(date,"/")
            #convert date into a number
            newdate=int(string.join([year,month_dict[month],day],""))
        except:
            continue
        logsrecord=re.match("^/logs/(.*)",request)
        #creat a tuple based on all the values from the line
        if len(record) > 5:            
            values=(record[0],newdate,time,int(zone),command,request,responseproto,int(record[3]),record[4],record[5],record[6],)
            if logsrecord == None:        
                cur.execute('''INSERT OR IGNORE INTO Logs (remote_user,date,time,timezone,command,request,responseproto,status,bytes_sent,referer,ua) VALUES(?,?,?,?,?,?,?,?,?,?,?)''',values ) 
        else:
            values=(record[0],newdate,time,int(zone),command,request,responseproto,int(record[3]),record[4],)
            print values
            if logsrecord == None:        
                cur.execute('''INSERT OR IGNORE INTO Logs (remote_user,date,time,timezone,command,request,responseproto,status,bytes_sent) VALUES(?,?,?,?,?,?,?,?,?)''',values )
        conn.commit()                	
    logfile.close()

#download the logs from http location
def downloadlogs(url):	
	#create the data dir if it doesn't exit'
    if not os.path.exists("data"):
        os.makedirs("data")
    index=urllib.urlopen(url)
    #read the file list from the url
    filelist=index.read()
    soup=BeautifulSoup(filelist)
    tags = soup('a')
    #download the file and make an entry in the DB with its name and processing status
    for tag in tags:
        href = tag.get('href', None)
        if href.endswith('log') :
            loguri=urljoin(url,href)
            logfile=string.join(["data",href],"/")            
            urllib.urlretrieve(loguri,logfile)
            cur.execute('''INSERT OR IGNORE INTO Logfiles (fileuri,status) VALUES (?,)''',(href,)) 

#connect to sqlite3 db                                        
conn = sqlite3.connect('accesslogs.sqlite')
cur = conn.cursor()
#create tables for all data from files to SQL
cur.execute('''CREATE TABLE IF NOT EXISTS Logs (id INTEGER PRIMARY KEY AUTOINCREMENT, remote_user TEXT , date INT,time TEXT, timezone INTEGER, command TEXT, request TEXT, responseproto TEXT,status INTEGER, bytes_sent TEXT, referer TEXT,ua TEXT)''')
cur.execute('''CREATE TABLE IF NOT EXISTS LogsProcessed (id INTEGER PRIMARY KEY UNIQUE, location INTEGER DEFAULT 0, status INTEGER DEFAULT 0, useragent INTEGER DEFAULT 0)''')
cur.execute('''CREATE TABLE IF NOT EXISTS LogFiles (id INTEGER PRIMARY KEY AUTOINCREMENT, fileuri TEXT, status INTEGER)''')
                
#parse command line arguments                
parser = argparse.ArgumentParser(description="Read Apache Logs and push into sqlite")
source = parser.add_mutually_exclusive_group(required=True)
source.add_argument('--data', help='Data Directory location',type=str)
source.add_argument('--url', help='URL Location',type=str)
source.add_argument('--resetLogs',help='Set All Logs to be unprocessed',choices=['0','1'])
args = parser.parse_args()   


if args.url != None:
    downloadlogs(args.url)
    args.data="data"

#reprocess all logs by marking them unprocessed    
if args.resetLogs == "1":
    cur.execute('''UPDATE LogsProcessed set location=0''')
    conn.commit()
    sys.exit(1)    
    
#check if the data entered is a file or dir and create the processing status table for all files
if os.path.isfile(args.data):
    logfile=string.split(args.data,"/")[-1]
    cur.execute('''SELECT status from LogFiles where fileuri=? ''',(logfile,))
    done=cur.fetchone()
    if  done == None or ( done != None and done[0] != 1):
        cur.execute('''INSERT OR IGNORE INTO LogFiles (fileuri,status) VALUES (?,?) ''',(logfile,0,))
        read(args.data  )
        cur.execute('''UPDATE LogFiles set status = 1 where fileuri=? ''',(logfile,))
        conn.commit()
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

cur.execute('''INSERT OR IGNORE INTO LogsProcessed (id) SELECT id from Logs''')
conn.commit()
        
cur.close()           