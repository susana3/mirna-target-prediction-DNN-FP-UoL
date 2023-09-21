from ratelimit import limits, sleep_and_retry
from datetime import timedelta
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import redis


# Initialize redis database.
r = redis.Redis(host='localhost', port=6379, decode_responses=True, db=3)

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


# ---------- Helper functions to generate the redis keys to store the results.
def r_target(target):
    return str(target)


def r_target_match_name(target):
    return f'{r_target(target)}_name'


# ---------- Web scrapping functions.
@sleep_and_retry
@limits(calls=1, period=timedelta(seconds=10).total_seconds())
def get_target_name(target):
    """
        Fetches the Arabidopsis.org website to extract the respective gene symbol
        in the format required for this project.

        Parameters:
        -----------
            target (string): Name of the mRNA target.

        Returns:
        --------
            None.
    """

    # Base url for Arabidopsis.org [43].
    base_url = 'https://www.arabidopsis.org/'

    # Prepares the target name.
    target = target.split('.')[0]

    # Skips if the gene does not require name annotation.
    if not target.startswith('AT'):
        return

    # Prevents fetching twice.
    if r.get(r_target_match_name(target)):
        return

    url = f'{base_url}/servlets/TairObject?type=locus&name={target}'
    # print(f'Fetching: {url}')
    browser.get(url)
    # Gets and stores the name in the required nomenclature format.
    row_id = browser.find_element(By.XPATH,
                                  '/html/body/div/div[2]/table[2]/tbody/tr[3]/th')
    if 'Other names' in row_id.text:
        row_name = browser.find_element(By.XPATH,
                                        '/html/body/div/div[2]/table[2]/tbody/tr[3]/td[2]/table/tbody/tr/td')
        # By default, selects the shortest form of target name.
        alt_names = row_name.text.split(',')
        target_name = alt_names[0]
        for alt_name in alt_names:
            if len(alt_name) < len(target_name):
                target_name = alt_name

        # Stores the values in the local db.
        if target_name != '':
            r.set(r_target_match_name(target), target_name)



