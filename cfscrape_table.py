# This script will visit https://sec.report/Form/4 and get all the link URLS and save to a csv file

import os
import sys
import time
import datetime
import configparser
import logging
import logging.handlers
import argparse
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
from fake_useragent import UserAgent
import re
import csv
from dateutil import parser
import html5lib
import lxml
import random
import cfscrape
#from sqlalchemy import create_engine
#from sqlalchemy.exc import IntegrityError

#DB_CONNECTION_STRING = 'mysql+pymysql://yhkyxszt_capewell_user_updated:ENT66%%$43#s@184.154.190.82/yhkyxszt_Capewell_EOD'
#engine = create_engine(DB_CONNECTION_STRING, pool_recycle=1)

config = configparser.ConfigParser()
logger = logging.getLogger(__name__)

user_agent_list = [ 
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36', 
	'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36', 
	'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36', 
	'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36', 
	'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36', 
	'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36', 
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36', 
	'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36', 
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36', 
	'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36', 
	'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)', 
	'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko', 
	'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)', 
	'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko', 
	'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko', 
	'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko', 
	'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)', 
	'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko', 
	'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)', 
	'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko', 
	'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)', 
	'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)', 
	'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)' 
 ] 

if len(sys.argv) > 1:
    config_file = sys.argv[1]


def lookup(section, key):
    try:
        return config.get(section, key)
    except:
        return ""


def lookupint(section, key):
    try:
        return config.getint(section, key)
    except:
        return 0

def scrape(url):

	profiles = []

	tag = lookup('INPUT','tag')
	href = lookup('INPUT','href')
	att_key = lookup('INPUT','att_key')
	att_val = lookup('INPUT','att_val')
	base_url = lookup('INPUT','base_url')
	
	sleep_time = lookupint('DEFAULT','sleep_time')	
	retry_count = lookupint('DEFAULT','max_retry')
	wait_time = lookupint('BROWSER','wait_time')
	
	# default values if config is not set
	if(sleep_time == 0):
		sleep_time = 3
	if(retry_count == 0):
		retry_count = 10
	if(wait_time == 0):
		wait_time = 5

	scraper = cfscrape.create_scraper()  # returns a CloudflareScraper instance

	for count in range(1,retry_count+1):
		# open url in browser
		#try:
			#try: 
			#	ua = UserAgent()
			#	userAgent = ua.random
			#except:
			#userAgent = random.choice(user_agent_list)
			# open url in browser
			#browser.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": userAgent})
			#print(browser.execute_script("return navigator.userAgent;"))
			#headers = {'User-Agent': userAgent, 'Accept-Encoding': 'gzip, deflate'}
		#	resp = scraper.get(url)
			#browser.get(url)
			#browser.execute_script('document.title')
		#except Exception as e:
		#	logger.debug("failed to open url [%s] in browser (%s)", url, e)
			#exit(1)	
		#time.sleep(wait_time)
		resp = scraper.get(url)
		print(resp.status_code)
		# Parse HTML and save to BeautifulSoup object
		#soup = BeautifulSoup(browser.page_source, "html.parser")
		soup = BeautifulSoup(resp.content, "html.parser")
		print(soup.prettify())
		table = soup.find(tag, attrs={att_key:att_val})
		print(str(table))
		
		# exit loop if table is found
		if(table != None):
			break

	try:
		df = pd.read_html(str(table))[0]
	except:
		print("table data not found!", soup)
		exit(1)

	links = []
	for tr in table.findAll("tr"):
		trs = tr.findAll("td")
		for each in trs:
			try:
				link = base_url + each.find('a')['href']
				links.append(link)
			except:
				pass

	df['Link'] = links

	#### DF Dataframce for Table Data ###
	print(df)








	# split Date to extract YYYYMMDD
	new = df["Date"].str.split(" ", n = 1, expand = True)
	
	# add new column YYYYMMDD
	df["YYYYMMDD"] = new[0]

	# select 2 columns to be outputed
	profile = df[["YYYYMMDD", "Link"]]

	# return in delimited format
	return profile.to_csv(index=False,header=False,sep=config.get('OUTPUT','delimiter'))

def process():
	data = []
	lines = []

	out = config.get('OUTPUT','output_list')
	try:
		fh = open(out, 'a', newline='')
	except Exception as e:
		print("exception: ", e)
		logger.debug("exception (%s)", e)
		exit(1)

	file = config.get('INPUT','input_list')
	logger.info("loading search list [%s]", file)
	
	# load input file
	try:
		fp = open(file)
	except Exception as e:
		print("exception: ", e)
		logger.debug("exception (%s)", e)
		exit(1)

	# read lines
	for line in fp:
		lines.append(line)
	fp.close()
	
	# remove duplicates
	lines = list(dict.fromkeys(lines))
	
	# search lines
	for line in lines:
		data = scrape(line.strip())
		#print(data)
		fh.write(data)
		#fh.writelines("%s\n" % x for x in data)
		fh.flush()
	
	fh.close()

def exit(x):
	if(lookup('BROWSER','browser_enabled') == 'YES'):
		quit_browser()
	sys.exit(x)

def quit_browser():
	print("quitting Browser Instance!")
	logger.info("quitting Browser Instance!")
	browser.quit()


def init_browser():
	print("initializing Browser Instance!")
	logger.info("Initializing Browser Instance!")
	
	browser_path = lookup('BROWSER','browser_path')
	logger.debug("browser_path: %s", browser_path)
	driver_path = lookup('BROWSER','driver_path')
	logger.debug("driver_path: %s", driver_path)
	
	option = webdriver.ChromeOptions()
	
	if (lookup('BROWSER','headless') == 'YES'):
		option.add_argument("--headless")
		logger.debug("headless mode enabled")
	if (lookup('BROWSER','incognito') == 'YES'):
		option.add_argument("--incognito")
		logger.debug("incognito mode enabled")
	if (lookup('BROWSER','no-sandbox') == 'YES'):
		option.add_argument("--no-sandbox")
		logger.debug("--no-sandbox mode enabled")
	if (lookup('BROWSER','disable-gpu') == 'YES'):
		option.add_argument('--disable-gpu')
		logger.debug("--disable-gpu mode enabled")		
	if (lookup('BROWSER','fake-user-agent') == 'YES'):
		try: 
			ua = UserAgent(verify_ssl=False)
			userAgent = ua.random
		except:
			userAgent = random.choice(user_agent_list)
		option.add_argument(f'user-agent={userAgent}')
		logger.debug("--fake-user-agent added [%s]",userAgent)			

	#option.add_argument('--disable-extensions')
	#option.add_argument('--profile-directory=Default')
	#option.add_argument("--disable-plugins-discovery");
	#option.add_argument("start-maximized")
	#option.add_argument("--disable-blink-features")
	#option.add_argument("--disable-blink-features=AutomationControlled")
	#option.add_argument("--disable-javascript")
	#option.add_experimental_option('useAutomationExtension', False)
	#option.add_experimental_option("excludeSwitches", ["enable-automation", "ignore-certificate-errors", "safebrowsing-disable-download-protection", "safebrowsing-disable-auto-update", "disable-client-side-phishing-detection"])
	#option.add_experimental_option( "prefs",{'profile.managed_default_content_settings.javascript': 2})
	
	# Create new Instance of Browser
	global browser
	
	if(browser_path != ''):
		option.binary_location = browser_path
	if(driver_path != ''):
		browser = webdriver.Chrome(executable_path=driver_path, options=option)
	else:
		browser = webdriver.Chrome(options=option)

	#browser.delete_all_cookies()
	#browser.set_window_size(800,800)
	#browser.set_window_position(0,0)
	#print(browser.execute_script("return navigator.userAgent;"))
	#browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
	#browser.execute_script("Object.defineProperty(navigator, 'languages', {get: function() { return ['en-US', 'en']; }, })")
	#browser.execute_script("Object.defineProperty(navigator, 'plugins',  { get: function() { return [1, 2, 3, 4, 5]; }, })")
	#browser.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.53 Safari/537.36'})
	#browser.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/85.0.4183.102 Safari/537.36'})
	print(browser.execute_script("return navigator.userAgent;"))
	
	logger.info("browser has been initialized!")


def init_logger(date_fmt, log_level, log_dir, name):
	numeric_level = getattr(logging, log_level.upper(), None)
	if not isinstance(numeric_level, int):
		raise ValueError('Invalid log level: %s' % log_level)

	try:
		now = datetime.datetime.now()	
		logger.setLevel(numeric_level)
		rfh = logging.handlers.TimedRotatingFileHandler(filename=log_dir + '/' + name + '_'+ now.strftime('%Y%m%d')+ '.log', when='midnight')
		fmtr = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt=date_fmt)
		rfh.setFormatter(fmtr)
		logger.addHandler(rfh)
	except Exception as e:
		print("failed to initialize logger (",str(e),")")
		sys.exit(1)
		
	logger.info("logger has been initialized!")


def load_config(file):
	print("loading config file [",file,"]")

	# load config file
	try:
		fp = open(file)
	except Exception as e:
		print("failed to open config file [",file,"]")
		print(str(e))
		sys.exit(1)
		
	config.read(file)

	# check data directory
	data_dir = config.get('LOG','DATA_DIR')
	if (not os.path.exists(data_dir)):
		print("creating directory [",data_dir,"]")
		os.makedirs(data_dir+'/')

	# check log directory
	log_dir = config.get('LOG','LOG_DIR')
	if (not os.path.exists(log_dir)):
		print("creating directory [",log_dir,"]")
		os.makedirs(log_dir+'/')

	init_logger(config.get('LOG','LOG_DATE_FMT'), config.get('LOG','LOG_LEVEL'), config.get('LOG','LOG_DIR'), config.get('LOG','LOG_FILE_PREFIX'))
	
	logger.debug("displaying config values!")

	for section_name in config.sections():
		logger.debug("[SECTION]: %s", section_name)
		logger.debug("  Options:%s", config.options(section_name))
		for name, value in config.items(section_name):
			logger.debug("  %s = %s", name, value)

	logger.info("config file [%s] has been loaded!", file)

def main():

	parser = argparse.ArgumentParser(description='Scrape Table v1.0')
	parser.add_argument('config_file', type=argparse.FileType('r'),
                    help='configuration file')
	args = parser.parse_args()	
	load_config(config_file)
	#init_browser()	
	process()
	#quit_browser()


if __name__ == '__main__':
    main()
