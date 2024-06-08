import glob
import os
import urllib.request
import re
from subprocess import check_call

from bs4 import BeautifulSoup
import zipfile
from tqdm import tqdm
import xml.etree.ElementTree as et
import concurrent.futures
import cycle


def list_crawl(url, match):
    charts = []
    html_page = urllib.request.urlopen(url)
    soup = BeautifulSoup(html_page, "html.parser")
    for link in tqdm(soup.findAll('a'), desc="Scanning website links"):
        link_x = link.get('href')
        if link_x is None:
            continue
        if re.search(match, link_x):
            charts.append(link_x)
    list_set = set(charts)  # unique
    return list(list_set)


def download(url):
    name = url.split("/")[-1]
    # check if exists
    if not os.path.isfile(name):
        urllib.request.urlretrieve(url, name)
    if name.endswith(".zip") or name.endswith(".ZIP"):  # if a zipfile, unzip first
        with zipfile.ZipFile(name, 'r') as zip_ref:
            zip_ref.extractall(".")


def download_list(charts):
    for cc in tqdm(range(len(charts)), desc="Downloading/unzipping"):
        download(charts[cc])


def call_script(script):
    check_call([script], shell=True)


def call_perl_script(script):
    check_call(["perl" + " " + script + ".pl > " + script + ".csv"], shell=True)


def read_dcs_xml():
    xml_file = glob.glob("afd_*.xml")[0]
    tree = et.parse(xml_file)
    root = tree.getroot()
    # Find all "airport" elements
    airport_elements = root.findall(".//airport")
    return airport_elements


def process_dcs(airport):
    aptid = airport.find('aptid').text
    pages = airport.find('pages')
    pdfs = pages.findall('pdf')

    if aptid is None:
        return

    apt_dir = "afd/" + aptid
    try:
        os.mkdir(apt_dir)
    except FileExistsError:
        pass

    # page is a new CS e.g. notices
    page = 0
    for pdf in pdfs:
        fn = pdf.text.upper()

        tokens = fn.split("_")
        base = ("CS-" + tokens[0]).upper()  # add region to name

        cmd = f'mogrify -trim +repage -dither none -antialias -density 225 -depth 8 -background white  -alpha remove -alpha off -colors 15 -format png -quality 100 -write {apt_dir}/{base}_{page}.png {fn}'
        call_script(cmd)
        page = page + 1


def make_dcs():
    try:
        os.mkdir("afd")
    except FileExistsError:
        pass

    # make all files upper case, FAA mixes cases
    files = glob.glob("*.pdf")
    for file in files:
        os.rename(file, file.upper())

    airports = read_dcs_xml()

    # submit 8 jobs at a time
    sub_lists = [airports[i:i + 8] for i in range(0, len(airports), 8)]

    for sublist in tqdm(sub_lists):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_dcs, elem) for elem in sublist]
            # Collect the results
            [future.result() for future in concurrent.futures.as_completed(futures)]


def zip_dcs():
    # US geo regions
    regions = ["AK", "PAC", "NW", "SW", "NC", "EC", "SC", "NE", "SE"]
    zip_files = []
    manifest_files = []

    for region in regions:
        try:
            os.remove("CS_" + region + ".zip")
            os.remove("CS_" + region)
        except FileNotFoundError as e:
            pass

    for region in regions:
        zip_files.append(zipfile.ZipFile("CS_" + region + ".zip", "w"))
        manifest_files.append(open("CS_" + region, "w+"))

    for ff in manifest_files:
        ff.write(cycle.get_cycle() + "\n")

    for count in range(len(regions)):
        file_list = glob.glob("*/CS-" + regions[count] + "_*", root_dir="afd/", recursive=True)
        for ff in tqdm(file_list, desc="Zipping CS-" + regions[count]):
            zip_files[count].write("afd/" + ff)
            manifest_files[count].write("afd/" + ff + "\n")

    for ff in manifest_files:
        ff.close()

    for count in range(len(regions)):
        zip_files[count].write("CS_" + regions[count])
        zip_files[count].close()
