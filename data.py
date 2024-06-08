import os
import zipfile

from tqdm import tqdm
import common

import cycle

# download
start_date = cycle.get_version_start(cycle.get_cycle_download())  # to download which cycle
all_charts = [
    "https://nfdc.faa.gov/webContent/28DaySub/28DaySubscription_Effective_" + start_date + ".zip",
    "https://nfdc.faa.gov/webContent/28DaySub/" + start_date + "/aixm5.0.zip",
    "https://aeronav.faa.gov/Obst_Data/DAILY_DOF_DAT.ZIP",
    "https://www.outerworldapps.com/WairToNowWork/avare_aptdiags.php"
]
common.download_list(all_charts)

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