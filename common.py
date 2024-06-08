import glob
import os
import urllib.request
import re
from subprocess import check_call

from bs4 import BeautifulSoup
import zipfile
from tqdm import tqdm

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


def make_main_vrt(vrt_list, chart_type):
    name = chart_type + ".vrt"
    try:
        os.remove(name)
    except FileNotFoundError as e:
        pass

    all_vrts = "".join([" '" + vrt + "' " for vrt in vrt_list])
    check_call(["gdalbuildvrt -r cubicspline -srcnodata 51 -vrtnodata 51 -resolution highest -overwrite " + name + all_vrts], shell=True)




def zip_files(list_of_all_tiles, chart):
    # US geo regions

    try:
        os.remove("AK_" + chart + ".zip")
        os.remove("HI_" + chart + ".zip")
        os.remove("NW_" + chart + ".zip")
        os.remove("SW_" + chart + ".zip")
        os.remove("NC_" + chart + ".zip")
        os.remove("SC_" + chart + ".zip")
        os.remove("NE_" + chart + ".zip")
        os.remove("SE_" + chart + ".zip")
        os.remove("AK_" + chart)
        os.remove("HI_" + chart)
        os.remove("NW_" + chart)
        os.remove("SW_" + chart)
        os.remove("NC_" + chart)
        os.remove("SC_" + chart)
        os.remove("NE_" + chart)
        os.remove("SE_" + chart)
    except FileNotFoundError as e:
        pass

    ak_file = zipfile.ZipFile("AK_" + chart + ".zip", "w")
    hi_file = zipfile.ZipFile("HI_" + chart + ".zip", "w")
    nw_file = zipfile.ZipFile("NW_" + chart + ".zip", "w")
    sw_file = zipfile.ZipFile("SW_" + chart + ".zip", "w")
    nc_file = zipfile.ZipFile("NC_" + chart + ".zip", "w")
    sc_file = zipfile.ZipFile("SC_" + chart + ".zip", "w")
    ne_file = zipfile.ZipFile("NE_" + chart + ".zip", "w")
    se_file = zipfile.ZipFile("SE_" + chart + ".zip", "w")
    ak_file_manifest = open("AK_" + chart, "w+")
    hi_file_manifest = open("HI_" + chart, "w+")
    nw_file_manifest = open("NW_" + chart, "w+")
    sw_file_manifest = open("SW_" + chart, "w+")
    nc_file_manifest = open("NC_" + chart, "w+")
    sc_file_manifest = open("SC_" + chart, "w+")
    ne_file_manifest = open("NE_" + chart, "w+")
    se_file_manifest = open("SE_" + chart, "w+")

    ak_file_manifest.write(cycle.get_cycle() + "\n")
    hi_file_manifest.write(cycle.get_cycle() + "\n")
    nw_file_manifest.write(cycle.get_cycle() + "\n")
    sw_file_manifest.write(cycle.get_cycle() + "\n")
    nc_file_manifest.write(cycle.get_cycle() + "\n")
    sc_file_manifest.write(cycle.get_cycle() + "\n")
    ne_file_manifest.write(cycle.get_cycle() + "\n")
    se_file_manifest.write(cycle.get_cycle() + "\n")

    ak = (-180, 71, -126, 51)
    hi = (-162, 24, -152, 18)
    nw = (-125, 50, -105, 40)
    sw = (-125, 40, -105, 15)
    nc = (-105, 50, -80,  40)
    sc = (-105, 40, -80,  15)
    ne = (-80,  50, -60,  40)
    se = (-80,  40, -60,  15)

    for tile in tqdm(list_of_all_tiles, desc="Zipping up tiles in Areas"):
        tokens = tile.split("/")
        y_tile = int(tokens[len(tokens) - 1].split(".")[0])
        x_tile = int(tokens[len(tokens) - 2])
        z_tile = int(tokens[len(tokens) - 3])
        lon_tile, lat_tile, lon1_tile, lat1_tile = projection.findBounds(x_tile, y_tile, z_tile)
        # include 7 and below in every chart
        if is_in(ak, lon_tile, lat_tile) or z_tile <= 7:
            ak_file.write(tile)
            ak_file_manifest.write(tile + "\n")
        if is_in(hi, lon_tile, lat_tile) or z_tile <= 7:
            hi_file.write(tile)
            hi_file_manifest.write(tile + "\n")
        if is_in(nw, lon_tile, lat_tile) or z_tile <= 7:
            nw_file.write(tile)
            nw_file_manifest.write(tile + "\n")
        if is_in(sw, lon_tile, lat_tile) or z_tile <= 7:
            sw_file.write(tile)
            sw_file_manifest.write(tile + "\n")
        if is_in(nc, lon_tile, lat_tile) or z_tile <= 7:
            nc_file.write(tile)
            nc_file_manifest.write(tile + "\n")
        if is_in(sc, lon_tile, lat_tile) or z_tile <= 7:
            sc_file.write(tile)
            sc_file_manifest.write(tile + "\n")
        if is_in(ne, lon_tile, lat_tile) or z_tile <= 7:
            ne_file.write(tile)
            ne_file_manifest.write(tile + "\n")
        if is_in(se, lon_tile, lat_tile) or z_tile <= 7:
            se_file.write(tile)
            se_file_manifest.write(tile + "\n")

    ak_file_manifest.close()
    hi_file_manifest.close()
    nw_file_manifest.close()
    sw_file_manifest.close()
    nc_file_manifest.close()
    sc_file_manifest.close()
    ne_file_manifest.close()
    se_file_manifest.close()

    # write manifest
    ak_file.write("AK_" + chart)
    hi_file.write("HI_" + chart)
    nw_file.write("NW_" + chart)
    sw_file.write("SW_" + chart)
    nc_file.write("NC_" + chart)
    sc_file.write("SC_" + chart)
    ne_file.write("NE_" + chart)
    se_file.write("SE_" + chart)

    ak_file.close()
    hi_file.close()
    nw_file.close()
    sw_file.close()
    nc_file.close()
    sc_file.close()
    ne_file.close()
    se_file.close()


def call_script(script):
    check_call([script], shell=True)


def call_perl_script(script):
    check_call(["perl" + " " + script + ".pl > " + script + ".csv"], shell=True)
