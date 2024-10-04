from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import uuid
import sys
from typing import Tuple
import re
from dateutil import parser
from time import sleep
import random
from scraping_utils import (
    write_db_artist_obj, write_db_asset_obj, write_db_info_log,
    I_DB_LOG, SCRAPED_DB_RECORD_TYPE, I_ScrapedArtistItem, I_ScrapedAssetItem, check_artist_url_existed, write_db_artist_url
)

retry_threshold = 3
root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_dir)

class ArtsAndCulture:
    def __init__(self,
                 driver,
                 db_collection_conn,
                 _collection_profile_id,
                 _ec2_artist_res_event_publish,
                 _ec2_asset_res_event_publish,
                 _ec2_log_publish,
                 _ec2_req_ip,
                 ):
        self.driver = driver
        self.db_collection_conn = db_collection_conn
        self.collection_profile_id = _collection_profile_id
        self.ec2_artist_res_event_publish = _ec2_artist_res_event_publish
        self.ec2_asset_res_event_publish = _ec2_asset_res_event_publish
        self.ec2_log_publish = _ec2_log_publish
        self.ec2_req_ip = _ec2_req_ip
        self.valid_event_order_id = 0
        self.scraping_continue = True

    def scrape_all_information(self):
        driver = self.driver

        def convert_string_to_number(s):
            if 'k' in s:
                multiplier = 1000
                s = s.replace('k', '')
            else:
                multiplier = 1
            pattern = r'[^0-9.]'
            result = re.sub(pattern, '', s)
            try:
                result = int(float(result) * multiplier)
            except ValueError:
                result = None
            return result

        def missing_day_or_month(date_str):
            full_date_pattern = r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)?\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{1,4}\b|\b\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)?,?\s+\d{1,4}\b"

            return not bool(re.search(full_date_pattern, date_str, re.IGNORECASE))

        def is_year_only(date_str):
            full_date_pattern = r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)?\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{1,4}\b|\b\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)?,?\s+\d{1,4}\b"
            year_only_pattern = r"^\d{1,4}$"

            return not bool(re.search(full_date_pattern, date_str, re.IGNORECASE)) and bool(
                re.match(year_only_pattern, date_str))

        def convert_date(date_str):
            date_str = date_str.lower()
            if 'c.' in date_str:
                date_str = date_str.replace('c.', '').strip()

            if 'bc' in date_str:
                date_str = date_str.replace('bc', '').strip()
                date_obj = parser.parse(date_str, default=datetime(2024, 1, 1))
                date_time_field = None
                if missing_day_or_month(date_str):
                    if is_year_only(date_str):
                        print(date_str)
                        date_time_str = date_obj.strftime('-%Y')
                    else:
                        date_time_str = date_obj.strftime('-%Y-%m')
                else:
                    date_time_str = date_obj.strftime('-%Y-%m-%d')

                return date_time_field, date_time_str

            date_obj = parser.parse(date_str, default=datetime(2024, 1, 1))

            if date_obj.year < 1000:
                date_time_field = None
                if missing_day_or_month(date_str):
                    if is_year_only(date_str):
                        date_time_str = date_obj.strftime('%Y')
                    else:
                        date_time_str = date_obj.strftime('%Y-%m')
                else:
                    date_time_str = date_obj.strftime('%Y-%m-%d')
                return date_time_field, date_time_str

            elif missing_day_or_month(date_str):
                date_time_field = date_obj.strftime('%Y-%m-%d')
                if is_year_only(date_str):
                    date_time_str = date_obj.strftime('%Y')
                else:
                    date_time_str = date_obj.strftime('%Y-%m')

                return date_time_field, date_time_str

            else:
                date_time_field = date_obj.strftime('%Y-%m-%d')
                date_time_str = date_obj.strftime('%Y-%m-%d')
                return date_time_field, date_time_str

        def extractArtistInfo(artist_url: str, artist_id:str, retry: int = retry_threshold) -> Tuple[bool, I_ScrapedArtistItem]:
            if retry == 0:
                return False, I_ScrapedArtistItem()

            try:
                print('Start Artist info for this url: ',artist_url)
                driver.get(artist_url)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                try:
                    artist_name = soup.find('h1').text
                except:
                    raise Exception('Artist Name not found, page load error')
                try:
                    avtar_url = 'https:' + soup.find('div', class_='BXbpw UXrsZb lXkFp jtk3Zb').find('div')['data-bgsrc']
                except:
                    avtar_url = None
                try:
                    artist_birth_date = soup.find('h2').text.split('-')[0].strip()
                    date_of_birth, date_of_birth_string = convert_date(artist_birth_date)
                except:
                    date_of_birth, date_of_birth_string = None, None
                try:
                    artist_death_date = soup.find('h2').text.split('-')[1].strip()
                    date_of_death, date_of_death_string = convert_date(artist_death_date)
                except:
                    date_of_death, date_of_death_string = None, None
                try:
                    artist_bio = soup.find('div', class_='zzySAd gI3F8b').text
                except:
                    artist_bio = None
                try:
                    total_artworks = soup.find('h3', class_='TzXVdf').text.split()[0]
                    total_artworks = convert_string_to_number(total_artworks)
                except:
                    total_artworks = 0

                time_stamp = str(datetime.now().timestamp() * 1000)
                sleep(random.randint(1, 5))
                di= {'artistName': artist_name, 'avtarURL':avtar_url , 'artistBirthDate': date_of_birth,
                      'artistBirthDateString':date_of_birth_string, 'artistDeathDate': date_of_death,
                      'artistDeathDateString':date_of_death_string, 'artistBio': artist_bio, 'totalArtwork': total_artworks,
                      'timeStamp': time_stamp}

                return True, I_ScrapedArtistItem(
                    artist_id=artist_id,
                    artist_name=di['artistName'],
                    avatar_url=di['avtarURL'],
                    date_of_birth=di['artistBirthDate'],
                    date_of_birth_string=di['artistBirthDateString'],
                    date_of_death=di['artistDeathDate'],
                    date_of_death_string=di['artistDeathDateString'],
                    artist_bio=di['artistBio'],
                    page_link=artist_url,
                    total_designs=int(di['totalArtwork']),
                    scraped_timestamp=di['timeStamp'])

            except Exception as err:
                print('!!! Fetch Artist Info Error, retrying remaining' + str(retry) + '=', str(err))
                return extractArtistInfo(artist_url, artist_id, retry - 1)

        def extractAssetInfo(asset_url: str, asset_img_url:str, artist_id: str, retry: int = retry_threshold) -> Tuple[bool, I_ScrapedAssetItem]:
            if retry == 0:
                return False, I_ScrapedAssetItem()
            try:
                # driver.get(asset_url)
                # soup = BeautifulSoup(driver.page_source, 'html.parser')
                response = requests.get(asset_url)
                soup = BeautifulSoup(response.content, 'html.parser')
                try:
                    asset_title = soup.find('h1').text
                except:
                    asset_title = None
                    pass
                try:
                    asset_year = soup.find('h2').find('span', class_='QtzOu').text
                    if '-' in asset_year:
                        asset_year = asset_year.split('-')[0].strip()
                    asset_created_date, asset_created_date_string = convert_date(asset_year)
                except:
                    asset_created_date, asset_created_date_string = None, None
                    pass
                try:
                    asset_artist_name = soup.find('h2').find('span', class_='QIJnJ').text
                except:
                    asset_artist_name = None
                    pass
                try:
                        asset_description = soup.select_one("meta[name='description']").get("content")
                except:
                    try:
                        asset_description = soup.find('div', class_='R5VDUc').text
                    except:
                        asset_description = None
                        pass        
               

                try:
                    asset_location_tag = soup.find('h3', class_='To7WBf')
                    asset_museum_name = asset_location_tag.get_text(separator='|', strip=True).split('|')[0]
                    asset_location = asset_location_tag.find('span', class_='WrfKPd').get_text(strip=True)
                except:
                    asset_museum_name = None
                    asset_location = None
                    pass

                artist_full_name, creator_life_span, artist_death_place, artist_birth_place, asset_date, tag, asset_dimension, asset_history, asset_type, asset_external_link, asset_medium, subject_keywprd, signatures, object_type, object_link, asset_medium, asset_credit_line, asset_material_technique, image_credit_line, asset_edition, asset_dimensions, asset_bibliography, associated_people = None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None
                try:
                    for li in soup.find('div', class_='ve9nKb').find_all('li'):
                        key = li.get_text(separator='|', strip=True).split('|')[0]
                        value = li.get_text(separator='|', strip=True).split('|')[1]
                        if key == 'Creator:':
                            artist_full_name = value
                        elif key == 'Creator lifespan:':
                            creator_life_span = value
                        elif key == "Creator's place of death:":
                            artist_death_place = value
                        elif key == "Creator's place of birth:":
                            artist_birth_place = value
                        elif key == 'Date created:':
                            asset_date = value
                        elif key == 'Tag:':
                            tag = value
                        elif key == 'Physical Dimensions:':
                            asset_dimension = value
                        elif key == 'Artwork history:':
                            asset_history = value
                        elif key == 'Type:':
                            asset_type = value
                        elif key == 'External Link:':
                            asset_external_link = value
                        elif key == 'Medium:':
                            asset_medium = value
                        elif key == 'Subject Keywords:':
                            subject_keywprd = value
                        elif key == 'Signatures / Inscriptions:':
                            signatures = value
                        elif key == 'Object Type:':
                            object_type = value
                        elif key == 'Object Link:':
                            object_link = value
                        elif key == 'Medium:':
                            asset_medium = value
                        elif key == 'Object Credit Line:':
                            asset_credit_line = value
                        elif key == 'Materials & Techniques:':
                            asset_material_technique = value
                        elif key == 'Image Credit Line:':
                            image_credit_line = value
                        elif key == 'Edition:':
                            asset_edition = value
                        elif key == 'Bibliography:':
                            asset_bibliography = value
                        elif key == 'Associated People:':
                            associated_people = value
                except:
                    pass
                time_stamp = str(datetime.now().timestamp() * 1000)
                sleep(random.randint(1, 5))
                di = {'assetTitle': asset_title,'assetURL':asset_url, 'assetYear': asset_created_date, 'assetYearString': asset_created_date_string, 'assetArtistName': asset_artist_name,
                      'assetDescription': asset_description, 'assetMuseumName': asset_museum_name, 'assetLocation': asset_location, 'timeStamp': time_stamp,
                      'artistFullName': artist_full_name, 'creatorLifeSpan': creator_life_span, 'artistDeathPlace': artist_death_place,
                      'artistBirthPlace': artist_birth_place, 'assetDate': asset_date, 'tag': tag, 'assetDimension': asset_dimension,
                      'assetHistory': asset_history, 'assetType': asset_type, 'assetExternalLink': asset_external_link,
                      'assetMedium': asset_medium, 'subjectKeyword': subject_keywprd, 'signatures': signatures, 'objectType': object_type,
                      'objectLink': object_link, 'assetCreditLine': asset_credit_line, 'assetMaterialTechnique': asset_material_technique,
                      'imageCreditLine': image_credit_line, 'assetEdition': asset_edition,
                      'assetBibliography': asset_bibliography, 'associatedPeople': associated_people}

                return True, I_ScrapedAssetItem(
                    asset_id=str(uuid.uuid4()),
                    asset_title=di['assetTitle'],
                    asset_img_url=asset_img_url,
                    asset_publication_link=di['assetURL'],
                    asset_created_date=di['assetYear'],
                    asset_created_date_string=di['assetYearString'],
                    asset_location=di['assetLocation'],
                    asset_description=di['assetDescription'],
                    asset_credit_line=di['assetCreditLine'],
                    asset_dimension=di['assetDimension'],
                    artist_ids=[artist_id],
                    scraped_timestamp=di['timeStamp'])

            except Exception as err:
                print('!!! Fetch Asset Info Error, retrying remaining' + str(retry) + '=', str(err))
                return extractAssetInfo(asset_url, asset_img_url, artist_id, retry - 1)

        def extractArtistAssetUrls(artist_url: str, artist_id:str, retry: int = retry_threshold):
            print('Asset info scraping started for ', artist_url)
            asset_urls = []
            if retry == 0:
                return asset_urls
            try:
                driver.get(artist_url)

                target_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//a[@class="e0WtYb kdYEFe ZEnmnd lXkFp PJLMUc"]'))
                )

                driver.execute_script("arguments[0].scrollIntoView();", target_element)

                sleep(3)
                asset_urls = []
                loop_try = 3
                while True:
                    if not self.scraping_continue:
                        print('scraping process terminated ..')
                        break
                    try:
                        element = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, '//*[@id="exp_tab_popular"]/div/div/div[2]/div')))
                        element.click()
                    except:
                        loop_try = loop_try - 1
                        pass
                    if loop_try == 0:
                        break
                    a_tags = driver.find_elements('xpath', '//div[@class="wcg9yf"]/div/a')
                    for a_tag in a_tags:
                        if not self.scraping_continue:
                            print('scraping process terminated ..')
                            break
                        a_tags_html = a_tag.get_attribute('outerHTML')
                        a_tags_soup = BeautifulSoup(a_tags_html, 'html.parser')
                        asset_url = 'https://artsandculture.google.com' + a_tags_soup.find('a').get('href')
                        asset_image_url = 'https:' + a_tags_soup.find('a').get('data-bgsrc')
                        asset_di = {asset_url: asset_image_url}
                        asset_urls.append(asset_di)
                        driver.execute_script("arguments[0].remove();", a_tag)
                        scrape_asset_success, res_asset_info = extractAssetInfo(asset_url, asset_image_url, artist_id)
                        print(res_asset_info)
                        if scrape_asset_success:
                            self.ec2_asset_res_event_publish(
                                self.ec2_req_ip,
                                self.collection_profile_id,
                                res_asset_info
                            )
                            write_db_asset_obj(self.db_collection_conn, res_asset_info)
                            self.valid_event_order_id += 1
                        asset_urls.append(asset_url)
                return asset_urls

            except Exception as err:
                print('!!! Fetch Asset URL Error, retrying remaining' + str(retry) + '=', str(err))
                return extractArtistAssetUrls(artist_url, artist_id, retry - 1)

        for i in range(224):
            if not self.scraping_continue:
                driver.quit()
                print('scraping process terminated ..')
                break
            formatted_number = str(i).zfill(3)
            response = requests.get(
                f'https://www.gstatic.com/culturalinstitute/sitemaps/artsandculture_google_com/sitemap-{formatted_number}.xml')
            soup = BeautifulSoup(response.content, 'xml')
            artists_locs = soup.select('loc:contains("/entity/")')
            for artists_loc in artists_locs:
                artist_url = artists_loc.text
                sleep(random.randint(1, 5))
                if not self.scraping_continue:
                    print('scraping process terminated ..')
                    break
                if check_artist_url_existed(self.db_collection_conn, artist_url):
                    print(f'Artist URL already scraped: {artist_url}')
                    continue
                artist_id = str(uuid.uuid4())
                scrape_artist_success, res_artist_info = extractArtistInfo(artist_url, artist_id)
                print(res_artist_info)
                if scrape_artist_success:
                    self.ec2_artist_res_event_publish(
                        self.ec2_req_ip,
                        self.collection_profile_id,
                        res_artist_info,
                    )
                    write_db_artist_obj(self.db_collection_conn, res_artist_info)
                    self.ec2_log_publish(
                        self.ec2_req_ip,
                        self.collection_profile_id,
                        I_DB_LOG(
                            log_data={
                                'artist_name': res_artist_info['artist_name'],
                                'artist_id': res_artist_info['artist_id'],
                            },
                            log_message='New Artist Scraped',
                            log_timestamp=str(datetime.now().timestamp()),
                            order_id=self.valid_event_order_id,
                        ),
                        SCRAPED_DB_RECORD_TYPE.INFO,
                    )
                    self.valid_event_order_id += 1

                    extractArtistAssetUrls(artist_url, artist_id)
                    # save scraping status history
                    write_db_artist_url(self.db_collection_conn, artist_url)

class SeminalScraper:
    _version = 1
    configs = {}

    def __init__(
            self,
            _collection_profile_id,
            _configs,
            _db_collection_conn,
            _ec2_artist_res_event_publish,
            _ec2_asset_res_event_publish,
            _ec2_log_publish,
            _ec2_req_ip
    ):
        # options = webdriver.ChromeOptions()
        # options.add_argument('--headless')
        # options.add_argument('--window-size=1920x1080')
        # options.add_argument('--no-sandbox')
        # options.add_argument('--disable-gpu')
        # options.add_argument('--disable-extensions')
        # options.add_argument('--disable-infobars')
        # options.add_argument("--timeout=100000")
        # self.asset_driver = uc.Chrome(options=options)

        options_driver = webdriver.ChromeOptions()
        options_driver.add_argument("--timeout=100000")
        options_driver.add_argument("--incognito")
        options_driver.add_argument("--user-data-dir=" + root_dir)
        self.driver = uc.Chrome(options=options_driver, use_subprocess=True)
        self.configs = _configs
        self.db_collection_conn = _db_collection_conn
        self.scraper = ArtsAndCulture(
            # self.asset_driver,
            self.driver,
            self.db_collection_conn,
            _collection_profile_id,
            _ec2_artist_res_event_publish,
            _ec2_asset_res_event_publish,
            _ec2_log_publish,
            _ec2_req_ip,
        )

    def stop_scraping(self):
        write_db_info_log(
            self.db_collection_conn,
            I_DB_LOG(
                log_data={},
                log_message='STOPPING ArtsAndCulture SCRAPER',
                log_timestamp=str(datetime.now().timestamp()),
                order_id=-1,
            ),
        )
        self.scraper.scraping_continue = False

    def start_scraping(self):
        print('scraping process started ..')
        write_db_info_log(
            self.db_collection_conn,
            I_DB_LOG(
                log_data={},
                log_message='SCRAPING ArtsAndCulture START',
                log_timestamp=str(datetime.now().timestamp()),
                order_id=-1,
            ),
        )
        self.scraper.scraping_continue = True
        self.scraper.scrape_all_information()
