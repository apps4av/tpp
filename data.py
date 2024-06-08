import os
import zipfile

from tqdm import tqdm
import common
import time

import cycle

# download
start_date = cycle.get_version_start(cycle.get_cycle_download())  # to download which cycle
all_charts = [
    "https://nfdc.faa.gov/webContent/28DaySub/28DaySubscription_Effective_" + start_date + ".zip",
    "https://nfdc.faa.gov/webContent/28DaySub/" + start_date + "/aixm5.0.zip",
    "https://aeronav.faa.gov/Obst_Data/DAILY_DOF_DAT.ZIP",
    "https://www.outerworldapps.com/WairToNowWork/avare_aptdiags.php",
]

all_charts_2 = common.list_crawl("https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/dafd/", "^http.*DCS_.*zip$")
for nn in all_charts_2:
    all_charts.append(nn)

common.download_list(all_charts)

# Do DCS
common.make_dcs()
common.zip_dcs()

with zipfile.ZipFile("SAA-AIXM_5_Schema/SaaSubscriberFile.zip", 'r') as zip_ref:
    zip_ref.extractall(".")
with zipfile.ZipFile("Saa_Sub_File.zip", 'r') as zip_ref:
    zip_ref.extractall(".")

for script in tqdm(["saa", "airport", "runway", "freq", "fix", "nav", "dof", "awos", "aw"], desc="Running PERL database files"):
    common.call_perl_script(script)

try:
    os.unlink("main.db")
except FileNotFoundError as e:
    pass
common.call_script("sqlite3 main.db < importother.sql")
