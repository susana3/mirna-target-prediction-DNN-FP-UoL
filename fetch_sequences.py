from ratelimit import limits, sleep_and_retry
from datetime import timedelta
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common import ElementNotVisibleException, ElementNotSelectableException
from selenium.webdriver.support import expected_conditions as EC
import redis


# Initialize redis database.
r = redis.Redis(host='localhost', port=6379, decode_responses=True, db=2)

# Configure Selenium.
user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ' \
             'AppleWebKit/537.36 (KHTML, like Gecko) ' \
             'Chrome/114.0.0.0] ' \
             'Safari/537.36'
options = Options()
options.add_argument(f'user-agent={user_agent}')
options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
ser = Service(r"/usr/local/bin/chromedriver")
browser = webdriver.Chrome(service=ser, options=options)


# ---------- Helper functions to generate the redis keys to store the sequence results.
def r_mirna(mirna):
    return str(mirna)


def r_mirna_sequence(mirna):
    return f'{r_mirna(mirna)}_sequence'


# ---------- Web scrapping functions.
@sleep_and_retry
@limits(calls=1, period=timedelta(seconds=10).total_seconds())
def get_mirbase_sequence(mirna):
    """
        Fetches the miRBase website to extract the miRNA sequences.

        Parameters:
        -----------
            mirna (string): Name of the miRNA.

        Returns:
        --------
            mirna_sequence (string): Retrieved nucleotide sequence.
    """

    # Base url for miRBase [42].
    base_url_mirbase = 'https://mirbase.org/'

    url = f'{base_url_mirbase}search/'
    print(f'Fetching: {url}\t - miRNA: {mirna}')
    browser.get(url)
    # Ensures the page is loaded.
    wait = WebDriverWait(browser,
                         timeout=10,
                         poll_frequency=1,
                         ignored_exceptions=[ElementNotVisibleException,
                                             ElementNotSelectableException])

    # Locates the Search button to send the request.
    search_button = wait.until(EC.element_to_be_clickable((By.XPATH,
                               '/html/body/div[1]/div/section/div[1]/form/button')))

    # Locates the input field to query the mirna by name.
    input_field = browser.find_element(By.XPATH, '/html/body/div[1]/div/section/div[1]/form/input')
    # Ensures the field has no previous data.
    input_field.clear()
    # Inserts the miRNA name to search.
    input_field.send_keys(mirna)

    # Performs the search.
    search_button.click()

    # Retrieves the table showing the search results.
    # By default, the first listed items in the table corresponds to the most accurate searches in the miRBase database,
    # hence the first listed mature sequence will be selected if several items match the searching criteria.
    time.sleep(3)
    results_table = browser.find_element(By.XPATH,
                                         '/html/body/div[1]/div[1]/div/div[3]/div[2]/div/div/table/tbody')
    for tr in results_table.find_elements(By.CSS_SELECTOR, 'tr'):
        # Checks if the miRNA is mature sequence.
        row_text = tr.text
        row_accession = row_text.split(' ')[1]
        if row_accession.startswith('MIMA'):
            # Selects the first mature sequence matching the search.
            mirna_link = tr.find_element(By.XPATH, 'td[1]/a')
            mirna_link.click()
            break

    # In the new rendered page, locates the row holding the sequence data.
    mirna_sequence = ''
    time.sleep(3)
    results_table_row = browser.find_element(By.XPATH,
                                             '/html/body/div[1]/div/table/tbody/tr[4]')
    if results_table_row.text.startswith('Sequence'):
        mirna_sequence = results_table_row.find_element(By.XPATH, 'td[2]').text

    return mirna_sequence


# Annotates the sequences.
def annotate_mirna_sequences(data):
    """
        Matches the miRNAs with their sequences, and stores the results into a local database.

        Parameters:
        -----------
            data (list): List containing miRNA name,
                         mature miRNA data from miRBase,
                         mature miRNA data from PmiRen.

        Returns:
        --------
            None
    """
    mirna = data[0]
    ath_mature_mirnas_dict = data[1]
    ath_mature_mirnas_pmiren_dict = data[2]

    if r.get(r_mirna_sequence(mirna)):
        # Only fetches the sequences that are not already stored into the local database.
        return

    if mirna in ath_mature_mirnas_dict.values():
        # miRNA is available at miRBase downloaded data.
        # So it extracts the sequence from the miRBase mature miRNAs dataset.
        mirna_sequence = list(filter(lambda x: ath_mature_mirnas_dict[x] == mirna,
                                     ath_mature_mirnas_dict))[0]
    elif mirna in ath_mature_mirnas_pmiren_dict.values():
        # miRNA is available at PmiRen downloaded dataset.
        mirna_sequence = list(filter(lambda x: ath_mature_mirnas_pmiren_dict[x] == mirna,
                                     ath_mature_mirnas_pmiren_dict))[0]
    else:
        # If the mirna cannot be found in the downloaded mature sequences datasets,
        # the sequence is extracted using web scrapping.
        mirna_sequence = get_mirbase_sequence(mirna)
        if not mirna_sequence:
            # If the sequence cannot be found in the miRBase website search tool, then is discarded.
            mirna_sequence = '-'

    # Saves the sequence into redis database.
    r.set(r_mirna_sequence(mirna), mirna_sequence)

