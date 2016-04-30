#!/usr/bin/python

from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import sqlite3
import sys

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
cur.execute('''SELECT longitude,latitude,frequency from Location''')
locations=cur.fetchall()
for point in locations:
    if point[0] != None: 
#    try:
        x,y = m(point[0],point[1])
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
