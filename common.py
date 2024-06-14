import glob
import os
import urllib.request
import re
from subprocess import check_call, check_output
from bs4 import BeautifulSoup
import zipfile
from tqdm import tqdm
import xml.etree.ElementTree as et
import concurrent.futures
import cycle
import pypdf

states_in_regions = {
    "AK":  ["AK",],
    "PAC": ["HI", "XX"],
    "NW":  ["WA", "MT", "WY", "ID", "OR"],
    "SW":  ["CA", "NV", "UT", "CO", "NM", "AZ"],
    "NC":  ["ND", "MN", "IA", "MO", "KS", "NE", "SD"],
    "EC":  ["WI", "MI", "OH", "IN", "IL"],
    "SC":  ["OK", "AR", "MS", "LA", "TX"],
    "NE":  ["NY", "ME", "VT", "NH", "MA", "RI", "CT", "NJ", "DE", "MD", "DC", "VA", "WV", "PA"],
    "SE":  ["KY", "NC", "SC", "GA", "FL", "AL", "TN", "PR", "VI"]
}


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


def call_script_return(script):
    return check_output([script], shell=True, encoding='utf8').strip()


def call_perl_script(script):
    check_call(["perl" + " " + script + ".pl > " + script + ".csv"], shell=True)


def process_plates(ad_tags, region):

    states = states_in_regions[region]

    tree = et.parse('d-TPP_Metafile.xml')
    root = tree.getroot()

    # all uppercase names
    files = glob.glob("*.pdf")
    for file in files:
        os.rename(file, file.upper())

    os.makedirs("plates", exist_ok=True)

    # only process this region
    all_states = root.findall('state_code')
    for state in tqdm(all_states, desc="Processing states"):
        if state.attrib["ID"] in states:
            process_plate_state(state, ad_tags)


def process_plate_city(city, state_id, ad_tags):
    for airport in city.findall('airport_name'):
        apt_id = airport.get('apt_ident')
        for record in airport.findall('record'):
            name = record.find('chart_name').text.upper()
            code = record.find('chart_code').text.upper()
            pdf = record.find('pdf_name').text.upper()
            out_name = code + "-" + state_id + "-" + name  # remove / from name
            out_name = out_name.replace("/", " AND ")
            dir_name = "plates/" + apt_id
            make_plate(dir_name, out_name, pdf, apt_id, ad_tags)


def process_plate_state(state, ad_tags):
    state_id = state.attrib["ID"]
    all_cities = state.findall('city_name')
    # submit 8 jobs at a time
    sub_lists = [all_cities[i:i + 8] for i in range(0, len(all_cities), 8)]

    for sublist in tqdm(sub_lists, desc="Processing cities of " + state_id):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_plate_city, elem, state_id, ad_tags) for elem in sublist]
            # Collect the results
            [future.result() for future in concurrent.futures.as_completed(futures)]


def parse_plate_size(string):
    string = string.replace(" ", "")
    match = re.match(r"^[a-zA-Z]*(\d+),(\d+)", string)
    return float(match.group(1)), float(match.group(2))


def parse_plate_coordinate(string):
    string = string.replace(" ", "")
    match = re.match(r"^[a-zA-Z0-9 ]+\(.*\)\((.*)\)", string)
    coordinate = match.group(1)
    match_lon = re.match(r"^([0-9]+)d([0-9]+)'([0-9.]+)\"([E|W]),([0-9]+)d([0-9]+)'([0-9.]+)\"([N|S])", coordinate)
    lon = (float(match_lon.group(1)) + float(match_lon.group(2)) / 60 + float(match_lon.group(3)) / 3600)
    if match_lon.group(4) == "W":
        lon = lon * -1
    lat = (float(match_lon.group(5)) + float(match_lon.group(6)) / 60 + float(match_lon.group(7)) / 3600)
    if match_lon.group(8) == "S":
        lat = lat * -1

    return lon, lat


def find_plate_pages(pdf_name, apt_id):
    pages = []
    reader = pypdf.PdfReader(pdf_name)
    string = r"\(" + apt_id + r"\)"
    string2 = r"\(K" + apt_id + r"\)"  # for K airports, FAA inconsistency
    # extract text and do the search
    for page in reader.pages:
        text = page.extract_text()
        res_search = re.search(string, text)
        res_search2 = re.search(string2, text)
        if (res_search is not None) or (res_search2 is not None):
            pages.append(page.page_number)
    return pages


def make_plate(folder, plate_name, plate_pdf, apt_id, ad_tags):
    # FAA sometimes adds files like DELETE_THIS.PDF in xml
    if not os.path.isfile(plate_pdf):
        print("\n **** Warning: File not found: " + plate_pdf + " ****\n")
        return

    os.makedirs(folder, exist_ok=True)

    png_file = "'" + folder + "/" + plate_name + ".png'"
    tif_file = png_file.replace(".png", ".tif")
    basic_options = "mogrify -quiet -dither none -antialias -depth 8 -quality 100 -background white -alpha remove -colors 15 -density 150 -format png "

    no_proj = call_script_return("gdalinfo " + plate_pdf)
    no_proj = "PROJCRS" not in no_proj

    if no_proj:
        if plate_name.startswith("APD-"):
            # add geotag in airport diagram
            comment = ad_tags.get(apt_id, "")
            call_script(basic_options + " -write " + png_file + " " + plate_pdf)
            call_script("exiftool -q -overwrite_original_in_place -UserComment='" + comment + "' " + png_file + " 2> /dev/null")

        elif plate_name.startswith("MIN-"):
            # only export relevant page
            pages = find_plate_pages(plate_pdf, apt_id)
            if len(pages) == 0:
                # these are probably radar minimums, add
                call_script(basic_options + "-write " + png_file + " " + plate_pdf)
            else:
                # T/O and ALT minimums
                index = 1
                for page in pages:
                    # do not replace with mogrify as that causes exception (probably due to delegate to gs)
                    call_script("gs -dNOPAUSE -dQUIET -dNOPROMPT -sDEVICE=pnggray -r150" + " -dFirstPage=" + str(page + 1) + " -dLastPage=" + str(page + 1) + " -o " + png_file.replace(".png", "-" + str(page) + ".png") + " " + plate_pdf)
                    index = index + 1
        else:
            # not a min, or apd, just include
            call_script(basic_options + "-write " + png_file + " " + plate_pdf)

    else:
        # geo tagged plate
        call_script("gdalwarp -q -r lanczos -t_srs epsg:3857 " + plate_pdf + " " + tif_file + " 2> /dev/null")
        tmp = call_script_return("gdalinfo " + tif_file).split("\n")
        upper_left = ([s for s in tmp if s.startswith("Upper Left")])
        lower_right = ([s for s in tmp if s.startswith("Lower Right")])
        size = ([s for s in tmp if s.startswith("Size")])
        (x, y) = parse_plate_coordinate(upper_left[0])
        (x0, y0) = parse_plate_coordinate(lower_right[0])
        (w, h) = parse_plate_size(size[0])
        comment = str(w / (x0 - x)) + '|' + str(h / (y0 - y)) + '|' + str(x) + '|' + str(y)
        # convert to png and add geotag to it under Comment
        call_script(basic_options + " " + tif_file)
        call_script("exiftool -q -overwrite_original_in_place -UserComment='" + comment + "' " + png_file + " 2> /dev/null")


def zip_plates(region):
    state_codes = states_in_regions[region]
    file_list = []
    for state in state_codes:
        state_list = glob.glob("plates/**/*-" + state + "-*.png", recursive=True)
        for ff in state_list:
            file_list.append(ff)

    try:
        os.remove(region + "_TPP" + ".zip")
    except FileNotFoundError as e:
        pass

    try:
        os.remove(region + "_TPP")
    except FileNotFoundError as e:
        pass

    zip_file = zipfile.ZipFile(region + "_TPP" + ".zip", "w")
    manifest_file = open(region + "_TPP", "w+")

    manifest_file.write(cycle.get_cycle() + "\n")

    for ff in tqdm(file_list, desc="Zipping " + region + "_TPP"):
        zip_file.write(ff)
        manifest_file.write(ff + "\n")

    manifest_file.close()
    zip_file.write(region + "_TPP")
    zip_file.close()

