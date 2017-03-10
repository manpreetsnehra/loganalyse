# loganalyse

logread.py : reads URL and downloads all files(apache logs to a directory), The directory is then processed file by file and an entry for each file is made into an sqlite table with the status check if its processed or not. Files are processed by extracting all the information from logfiles one line at a time and inserting them into sqlite table again. The hostnames are not resolved at this stage to speed up the process of insertion which happens line by line. 

findlocation.py: reads the log table created by logread.py and extracts the ip/hostname from the table and processes it 10000 entries at a time. Each entry is checked if its an IP, for ip address the geoipdb is looked up and location table is updated with longitude,latitude,city,country and frequency of occurance. It also ignores any unresolvable IPs in case the hostname was temporary or doesnt exist anymore. it also updates the logsprocessed table to mark which rows have been processed.

plotlocation.py: reads the location db and plots the longitude/latitude on a worldmap using matplotlib. if frequency is below white dot if its above blue dot.

status.py: Reads the log table and extracts info regarding various codes ex 200,301,404,501 and so on and puts them against each ip creating a frequency table per ip how many status reply of what type. It also calculates the data sent to each ip adding up all the data sent mentioned in the logs