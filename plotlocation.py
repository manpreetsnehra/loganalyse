#!/usr/bin/python
#plot the locations for findlocation on a world map
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import sqlite3
import sys
import argparse

parser = argparse.ArgumentParser(description="Number of Records to IP locations on World Map")
parser.add_argument('--ipcount', help='Number of Ips to plot on the map',type=str)
args = parser.parse_args()   

#Draw the base map with NASA bluemarble
m = Basemap(projection='cyl',llcrnrlat=-90,urcrnrlat=90,llcrnrlon=-180,urcrnrlon=180,resolution='l')
#m.bluemarble()
m.bluemarble(scale=1)
m.drawcountries()

try: 
    conn = sqlite3.connect('accesslogs.sqlite')
except:
    print "Logs database file not found"
    sys.exit(1)

cur=conn.cursor()
i=0
if args.ipcount == None:
    cur.execute('''SELECT longitude,latitude,frequency from Location''')
else:    
    cur.execute('''SELECT longitude,latitude,frequency from Location ORDER bY frequency Desc LIMIT ?''',(args.ipcount,))
    
locations=cur.fetchall()
for point in locations:
    if point[0] != None: 
#    try:
        x,y = m(point[0],point[1])
#Use 2 colors based on frequency of connection        
        if point[2] < 10:
            m.plot(x,y,'wh')
        else:
            m.plot(x,y,'bo')
        i= i+1
#    except:
#        continue
    
m.drawmapboundary(fill_color='aqua') 
plt.title("User Distribution Geographically")
plt.show()
