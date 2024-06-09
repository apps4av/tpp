import glob

import common

import cycle

start_date = cycle.get_version_start(cycle.get_cycle_download())  # to download which cycle

all_charts = common.list_crawl("https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/dtpp/", "^http.*DDTPP[A-E]+_" + start_date.replace("-", "")[2:] +  ".zip$")
all_charts.append("https://www.outerworldapps.com/WairToNowWork/avare_aptdiags.php")
# download
common.download_list(all_charts)

# make a list of airports for tagging
d = {}
with open("avare_aptdiags.php") as f:
    for line in f:
        (key, val0, val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11) = line.rstrip().split(",")
        d[str(key)] = val6 + "," + val7 + "," + val8 + "," + val9 + "," + val10 + "," + val11

common.process_plates(d)
common.zip_plates()






