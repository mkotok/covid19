# built-in modules 
import datetime
import glob
import os
import pickle
import re
import string
import sys

# web scraping modules
import requests
import bs4

# pdf text extraction module
import PyPDF2

# google sheets api modules
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# web scraping constants
DOMAIN = "https://www2.monroecounty.gov"
ARCHIVE_URL = "https://www2.monroecounty.gov/health-COVID-19-archive"
HREF_ROOT = "/files/health/coronavirus/"
PUNCTUATION = re.escape(string.punctuation)

# pdf parsing constants
PDF_DIR = "pdfs"
FILE_DT_FMT = "%Y-%m-%d_%H%M"
REGEXS = [
    r"There are (\d+) confirmed cases",
    r"Deaths related to COVID-19 +(\d+) patients?",
    r"(\d+) people are hospitalized",
    r"(\d+) (of|are).* in ICU",
    r"(\d+) people in(to)? mandatory quarantine",
]

# google sheets api constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SHEET_ID = "1o6NO41-rzpsmXTTtvXjLv6vMnNf4h8fmO5MflFPwDuQ"
SHEET_DT_FMT = "%Y-%m-%d %H:%M"
DATETIME_COL = 'A'
DATA_RANGE = 'A:G'


def main():
    # get index page source code "soup"
    soup = get_soup(ARCHIVE_URL)

    # get relavent update pdf links
    links = get_links(soup)

    # download new pdfs
    download_pdfs(links)

    # connect with google sheet service & get last updated datetime
    sheet_api = get_sheet_service()
    last_update = get_last_update(sheet_api, SHEET_ID, DATETIME_COL)

    # parse update pdfs
    info = parse_pdfs(PDF_DIR, last_update)

    # if no new info, update and exit
    if len(info) == 0:
        print("no new information to update")
        sys.exit(0)
    
    # append info to sheet
    append_to_sheet(sheet_api, info, SHEET_ID, DATA_RANGE)

###############################################################################

def get_soup(url):
    """get BeautifulSoup html object from url"""
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        sys.exit("ERROR: could not connect to: %s" % url)

    soup = bs4.BeautifulSoup(response.text, features="html.parser")
    return soup


def get_links(soup):
    """get all links with href root"""

    # initial empty dictionary
    links = {}

    # iterate over all links containing @href
    for a in soup.find_all('a', href=re.compile(HREF_ROOT)):
        text = a.text.lower()
        # replace non-printable characters with space
        text = re.sub(r'[^\x00-\x7f]', ' ', text)
        # replace punctuation with space
        text = re.sub('[%s]' % PUNCTUATION, ' ', text)
        # remove double spaces
        text = re.sub(' +', ' ', text)
        # remove links to statements & cases by zip code
        if re.search('statement|zip code', text):
            continue

        # ensure all href links are the full url
        href = a['href']
        if href.startswith('/'):
            href = DOMAIN + href

        # add to links dictionary
        links[text] = href

    # only use corrected links
    for text in list(links.keys()):
        # search for link text containing the word "corrected"
        if 'corrected' in text:
            # remove "corrected" string from text
            uncorrected = text.replace('corrected', '').strip()

            # replace uncorrected link with corrected one
            if uncorrected in links:
                links[uncorrected] = links[text]
                del links[text]
            else:
                sys.exit("ERROR: uncorrected link not found")

    # convert to better datetime format
    for text in list(links.keys()):
        if '2020' in text:
            dt = datetime.datetime.strptime(text, '%A %B %d %Y %I%p')
        else:
            dt = datetime.datetime.strptime(text, '%A %B %d %I%p')
            dt = dt.replace(year=2020)
        dt = dt.strftime(FILE_DT_FMT)
        links[dt] = links[text]
        del links[text]
                
    return links


def download_pdfs(links):
    """download all pdfs in dictionary @links to PDF_DIR"""

    # create PDF_DIR if necessary
    if not os.path.isdir(PDF_DIR):
        os.makedirs(PDF_DIR)

    # download new update pdfs
    for dt, url in links.items():
        filepath = os.path.join(PDF_DIR, dt + '.pdf')
        if not os.path.isfile(filepath):
            bname = url.replace(DOMAIN + HREF_ROOT, '')
            print("downloading %s <- %s" % (filepath, bname))
            download_pdf(url, filepath)


def download_pdf(url, filepath):
    """download url to filepath"""

    # get requests response
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        sys.exit("ERROR: could not access file: %s" % url)

    # write requests response content to file
    with open(filepath, 'wb') as ofs:
        ofs.write(response.content)


def get_sheet_service():
    """get sheet service through google api"""

    # see: https://developers.google.com/sheets/api/quickstart/python
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    sheet_api = service.spreadsheets()

    return sheet_api


def get_last_update(sheet_api, sheet_id, datetime_col):
    """read datetime col for latest update recorded"""

    # get values in datetime col
    sheet_range = datetime_col + "2:" + datetime_col
    result = sheet_api.values().get(
        spreadsheetId=sheet_id, range=sheet_range).execute()
    values = result.get('values', [])

    # get last entered datetime
    if len(values) > 0:
        dt = datetime.datetime.strptime(values[-1][0], SHEET_DT_FMT)
    else:
        dt = datetime.datetime.min

    print("last updated datetime: " + dt.strftime(SHEET_DT_FMT))
    return dt


def append_to_sheet(sheet_api, info, sheet_id, data_range):
    """append info data to sheet; must be same number of cols as data_range"""

    print("updating spreadsheet...")

    # form body dictionary to append
    body = {'values': info}

    # append to sheet
    sheet_api.values().append(
        spreadsheetId=sheet_id, range=data_range,
        valueInputOption="USER_ENTERED", body=body).execute()


def parse_pdfs(pdf_dir, last_update):
    """parse each *.pdf in @dir"""

    # get pdf names by glob & sort by datetime
    pdfs = glob.glob(os.path.join(pdf_dir, '*.pdf'))
    pdfs.sort()

    # initialize empty info list
    info = []

    # search pdf relevant text
    for pdf in pdfs:
        # datetime is recorded in filename; [:-4] to remove .pdf extension
        bname = os.path.basename(pdf)[:-4]
        dt = datetime.datetime.strptime(bname, FILE_DT_FMT)

        # skip pdfs before last_update
        if dt <= last_update:
            continue

        # parse text from pdf
        print("parsing info from: " + pdf)
        txt = parse_pdf(pdf)

        # initial cols for datetimes
        # None is to ignore cleaned date fmt column in sheet
        vals = [dt.strftime(SHEET_DT_FMT), None]

        # search for regexes
        for regex in REGEXS:
            vals.append(search_for(regex, txt))

        # append vals info tuple
        info.append(vals)
    
    return info


def search_for(regex, txt):
    """search for regex in txt and return match or NaN"""

    search = re.search(regex, txt, re.IGNORECASE)
    if search is not None:
        val = search.group(1)
    else:
        val = "n/a"

    return val


def parse_pdf(filepath):
    """read and extract text from pdf"""
    
    out = ''
    pdf = PyPDF2.PdfFileReader(filepath)
    for page in pdf.pages:
        txt = page.extractText()
        txt = txt.replace('\n', '')
        txt = re.sub('  +', '\n', txt)
        txt += '\n' + '-'*79 + '\n'

        out += txt

    return out


###############################################################################

if __name__ == "__main__":
    main()
