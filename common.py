import glob
import os
import urllib.request
import re
from subprocess import check_call
from osgeo import gdal
from bs4 import BeautifulSoup
import zipfile
from tqdm import tqdm
import xml.etree.ElementTree as et
import concurrent.futures
import cycle
import pypdf

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

    for sublist in tqdm(sub_lists, desc="Processing DCS"):
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


def make_data():
    with zipfile.ZipFile("SAA-AIXM_5_Schema/SaaSubscriberFile.zip", 'r') as zip_ref:
        zip_ref.extractall(".")
    with zipfile.ZipFile("Saa_Sub_File.zip", 'r') as zip_ref:
        zip_ref.extractall(".")

    # parse all FAA data
    for script in tqdm(["saa", "airport", "runway", "freq", "fix", "nav", "dof", "awos", "aw"],
                       desc="Running PERL database files"):
        call_perl_script(script)


def make_db():
    try:
        os.unlink("main.db")
    except FileNotFoundError as e:
        pass
    call_script("sqlite3 main.db < importother.sql")

    try:
        os.remove("databases.zip")
        os.remove("databases")
    except FileNotFoundError as e:
        pass

    zip_file = zipfile.ZipFile("databases.zip", "w")
    manifest_file = open("databases", "w+")
    manifest_file.write(cycle.get_cycle() + "\n")
    manifest_file.write("main.db\n")
    manifest_file.close()
    zip_file.write("databases")
    zip_file.write("main.db")
    zip_file.close()


def process_plates(ad_tags):
    tree = et.parse('d-TPP_Metafile.xml')
    root = tree.getroot()

    # all uppercase names
    files = glob.glob("*.pdf")
    for file in files:
        os.rename(file, file.upper())

    try:
        os.mkdir("plates")
    except FileExistsError:
        pass

    # submit 5 jobs at a time
    all_states = root.findall('state_code')
    sub_lists = [all_states[i:i + 5] for i in range(0, len(all_states), 5)]

    for sublist in tqdm(sub_lists, desc="Processing DCS"):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_plate_state, elem, ad_tags) for elem in sublist]
            # Collect the results
            [future.result() for future in concurrent.futures.as_completed(futures)]


def process_plate_state(state, ad_tags):
    state_id = state.attrib["ID"]
    for city in tqdm(state.findall('city_name'), desc="Processing " + state_id):
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


def parse_size(string):
    string = string.replace(" ", "")
    match = re.match(r"^[a-zA-Z]*(\d+),(\d+)", string)
    return float(match.group(1)), float(match.group(2))


def parse_coordinate(string):
    string = string.replace(" ", "")
    match = re.match(r"^[a-zA-Z0-9 ]+\(.*\)\((.*)\)", string)
    coords = match.group(1)
    match_lon = re.match(r"^([0-9]+)d([0-9]+)'([0-9.]+)\"([E|W]),([0-9]+)d([0-9]+)'([0-9.]+)\"([N|S])", coords)
    lon = (float(match_lon.group(1)) + float(match_lon.group(2)) / 60 + float(match_lon.group(3)) / 3600)
    if match_lon.group(4) == "W":
        lon = lon * -1
    lat = (float(match_lon.group(5)) + float(match_lon.group(6)) / 60 + float(match_lon.group(7)) / 3600)
    if match_lon.group(8) == "S":
        lat = lat * -1

    return lon, lat


def find_page(pdf_name, apt_id):
    reader = pypdf.PdfReader(pdf_name)
    num_pages = len(reader.pages)
    string = r"\(" + apt_id + r"\)"
    # extract text and do the search
    for page in reader.pages:
        text = page.extract_text()
        res_search = re.search(string, text)
        if res_search is not None:
            return page.page_number
    return -1


def make_plate(folder, plate_name, plate_pdf, apt_id, ad_tags):
    try:
        os.mkdir(folder)
    except FileExistsError as e:
        pass

    no_proj = gdal.Info(plate_pdf)
    no_proj = (no_proj.find("PROJCRS") < 0)

    if no_proj:
        if plate_name.startswith("APD-"):
            # add geotag in airport diagram
            try:
                comment = ad_tags[apt_id]
            except Exception as e:
                comment = ""
            call_script("mogrify -quiet -dither none -antialias -depth 8 -quality 00 -background white -alpha remove -colors 15 -format png -set Comment '" + comment + "' -write '" + folder + "/" + plate_name + ".png' " + plate_pdf)

        elif plate_name.startswith("MIN-"):
            # only export relevant page
            page = find_page(plate_pdf, apt_id)
            if page == -1:
                # these are probably radar minimums, add
                call_script("mogrify -dither none -antialias -depth 8 -quality 00 -background white -alpha remove -colors 15 -density 150 -format png -write '" + folder + "/" + plate_name + ".png' " + plate_pdf)
            else:
                # find if minimums file already exists
                file = plate_pdf.replace(".PDF", "") + "-" + str(page) + ".png"
                if os.path.isfile(file):
                    call_script("cp " + file + " '" + folder + "/" + plate_name + ".png'")

                else:
                    call_script("mogrify -dither none -antialias -depth 8 -quality 00 -background white -alpha remove -colors 15 -density 150 -format png " + plate_pdf)

        else:
            # left over, just include
            call_script("mogrify -dither none -antialias -depth 8 -quality 00 -background white -alpha remove -colors 15 -density 150 -format png -write '" + folder + "/" + plate_name + ".png' " + plate_pdf)

    else:
        # geo tagged plate
        call_script("gdalwarp -q -r lanczos -t_srs epsg:3857 " + plate_pdf + " '" + folder + "/" + plate_name + ".tif' > /dev/null")
        tmp = gdal.Info(gdal.Open(folder + "/" + plate_name + ".tif")).split("\n")
        upper_left = ([s for s in tmp if s.startswith("Upper Left")])
        lower_right = ([s for s in tmp if s.startswith("Lower Right")])
        size = ([s for s in tmp if s.startswith("Size")])
        (x, y) = parse_coordinate(upper_left[0])
        (x0, y0) = parse_coordinate(lower_right[0])
        (w, h) = parse_size(size[0])
        comment = str(w / (x0 - x)) + '|' + str(h / (y0 - y)) + '|' + str(x) + '|' + str(y)
        # convert to png and add geo tag to it under Comment
        call_script("mogrify -quiet -dither none -antialias -depth 8 -quality 00 -background white -alpha remove -colors 15 -format png -set Comment '" + comment + "' '" + folder + "/" + plate_name + ".tif'")


def zip_plates():
    pass