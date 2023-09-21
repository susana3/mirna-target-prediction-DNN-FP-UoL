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
r = redis.Redis(host='localhost', port=6379, decode_responses=True, db=4)

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
def r_target(target):
    return str(target)


def r_target_sequence(target):
    return f'{r_target(target)}_sequence'


# ---------- Web scrapping functions.
@sleep_and_retry
@limits(calls=1, period=timedelta(seconds=10).total_seconds())
def get_target_sequence(target):
    """
        Fetches the NCBI website to search for the target sequences.

        Parameters:
        -----------
            target (string): Name of the gene target transcript.

        Returns:
        --------
            target_sequence (string): Retrieved nucleotide sequence.
    """

    # Base url for NCBI nucleotide database [44].
    base_url_ncbi = 'https://www.ncbi.nlm.nih.gov/nuccore/'

    url = f'{base_url_ncbi}?term={target}'
    print(f'Fetching: {url}\t - target: {target}')
    browser.get(url)
    # Ensures the page is loaded.
    wait = WebDriverWait(browser,
                         timeout=10,
                         poll_frequency=1,
                         ignored_exceptions=[ElementNotVisibleException,
                                             ElementNotSelectableException])

    # Locates the 1st item in the results list.
    link = wait.until(
        EC.element_to_be_clickable((By.XPATH,
                                    '/html/body/div[1]/div[1]/form/div[1]/div[4]/div/div[5]/div[1]/div[2]/p/a'))
    )
    link.click()

    # Locates the section mentioning the sequence.
    fasta_button = wait.until(
        EC.element_to_be_clickable((By.XPATH,
                                    '/html/body/div[1]/div[1]/form/div[1]/div[4]/div/div[5]/div[1]/p[2]/span/a[1]'))
    )
    fasta_button.click()

    # Extracts the sequence in FASTA format.
    time.sleep(3)
    target_sequence = browser.find_element(By.XPATH,
                                           '/html/body/div[1]/div[1]/form/div[1]/div[4]/div/div[5]/div[2]/div[1]/pre')
    target_sequence = ''.join(target_sequence.text.split('\n')[1:])

    return target_sequence


def annotate_target_sequences(data):
    """
        Matches the targets with their sequences, and stores the results into a local database.

        Parameters:
        -----------
            data (list): List containing target name,
                         and the genome dictionary.

        Returns:
        --------
            None
    """

    target = data[0]
    ath_genome_dict = data[1]
    target_sequence = '-'

    if r.get(r_target_sequence(target)):
        # Only fetches the sequences that are not already stored into the local database.
        return

    if target in ath_genome_dict.values():
        # Target is available at the downloaded genome.
        # So it extracts the sequence from the dictionary.
        target_sequence = list(filter(lambda x: ath_genome_dict[x] == target,
                                      ath_genome_dict))[0]
    else:
        # If the target cannot be found in the downloaded genome,
        # then the sequence is extracted using web scrapping.
        target_sequence = get_target_sequence(target)
        if not target_sequence:
            # If the sequence cannot be found in the NCBI nucleotide
            # website search tool, then it is discarded.
            target_sequence = '-'

    # Saves the sequence into redis database.
    r.set(r_target_sequence(target), target_sequence)
