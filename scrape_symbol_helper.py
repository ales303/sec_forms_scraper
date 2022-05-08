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
import re
import random
from scrape_symbol_helper_generate_input_txt import update_sec_form_4_record_with_stock_data

config = configparser.ConfigParser()
logger  = logging.getLogger(__name__)

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
	rows = []
	dict = {}
	
	userAgent = random.choice(user_agent_list)
	browser.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": userAgent})
	print(browser.execute_script("return navigator.userAgent;"))
	
	# open url in browser
	browser.get(url)
	
	# sleep to load the URL
	time.sleep(config.getint('BROWSER','wait_time'))

	# Parse HTML and save to BeautifulSoup object
	soup = BeautifulSoup(browser.page_source, "html.parser")
	#print(soup.prettify())

	# iterate all data to scrape
	print("Scraping url: ", url)
	for x in config.get('OUTPUT','fields').split(','):
		key = x.strip()
		logger.debug("[%s]", key)
		tag = lookup(key,'tag')
		#print("tag: ", "=", tag)
		att_key = lookup(key,'att_key')
		#print("att_key: ", att_key)
		att_val = lookup(key,'att_val')
		#print("att_val: ", att_val)
		pattern = lookup(key,'pattern')
		#print("pattern: ", pattern)
		href = lookup(key,'href')
		#print("href: ", href)		
		
		if(pattern == ""):
			lists = soup.find_all(tag, attrs={att_key:att_val})
		else:
			if(att_key == ""):
				lists = soup.find_all(tag, text=re.compile(pattern))
			else:
				lists = soup.find_all(tag, text=re.compile(pattern), attrs={att_key:att_val})

		if(href != ""):
			lists = soup.find_all(tag, href=re.compile(href))


		# concat with new line
		val = ""
		for item in lists:
			#print(item)
			try:
				if(val == ""):
					val = item.text.strip()
				else:
					val = val + '\n' + item.text.strip()
			except:
				val = ""
			#print(val)
			#if(val != ""):
			#	break

		split = lookup(key,'split')
		if(split == 'YES'):
			delimiter = lookup(key,'delimiter')
			if(delimiter == 'space'):
				delimiter = ' '
			if(delimiter == 'newline'):
				delimiter = '\n'
			list = val.split(delimiter)
			
			join_char = lookup(key,'join_char')
			if(join_char == 'space'):
				join_char = ' '
			val = ""
			
			for x in lookup(key,'index').strip().split(','):
				#print("[",x,"]")
				try:
					val = val + join_char + list[int(x.strip())].strip()
				except:
					val = val + join_char + ""

			#print(val)

		remove_char = [x.strip() for x in lookup(key,'remove_char').split('|')]
		#print(remove_char)
		#print("|".join(remove_char))
		val = re.sub("|".join(remove_char), "", val)
		
		# remove string
		val = re.sub(lookup(key,'remove_string'),'',val)
		
		# remove non unicode characters
		val = re.sub(r'[^\x00-\x7F]','', val)
		#print(val)
		date_format = lookup(key,'date_format')		
		if date_format != '':
			d = parser.parse(val)
			#print(d.strftime(date_format))
			val = d.strftime(date_format)

		logger.debug("%s = %s", key, val)
		print(key, "=", val)
		dict[key]=val

	rows.append(dict)

	return rows

def process():
	data = []

	file = config.get('INPUT','url_list')
	logger.info("loading input list [%s]", file)
	
	# load input file
	try:
		fp = open(file)
	except Exception as e:
		logger.debug("exception (%s)", e)
		exit(1)

	# convert header columns to list
	fields = config.get('OUTPUT','fields')
	headers = [x.strip() for x in fields.split(',')]
	print("Column Headers: ", headers)


	# read lines
	for line in fp:
		url = line.strip()
		#print("Processing url: ", url)
		data = scrape(url)
		print(data)
		#symbol_string = url[40:44]
		#print(symbol_string)
		# Create DataFrame from scraped data
		df = pd.DataFrame(data,columns=headers)
		try:
			df['MARKET_CAP'] = df['MARKET_CAP_1K'].astype('int64') * 1000
			print(df.to_string(index=False))
			update_sec_form_4_record_with_stock_data(df['SYMBOL'].iloc[0], df['MARKET_CAP'].iloc[0], df['AVG_VOLUME'].iloc[0])
		except ValueError as e:
			print("Error occurred:", e)
		finally:
			print('\n')

		#logger.info(f"Finished updating table for symbol {symbol_string}")

	fp.close()

def exit(x):
	quit_browser()
	sys.exit(x)

def quit_browser():
	print("Quitting Browser Instance!")
	logger.info("Quitting Browser Instance! - Finished")
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
		ua = UserAgent(verify_ssl=False)
		userAgent = ua.random
		option.add_argument(f'user-agent={userAgent}')
		logger.debug("--fake-user-agent added [%s]",userAgent)			

	option.add_argument('--disable-extensions')
	option.add_argument('--profile-directory=Default')
	option.add_argument("--disable-plugins-discovery");
	option.add_argument("start-maximized")
	option.add_argument("--disable-blink-features")
	option.add_argument("--disable-blink-features=AutomationControlled")
	option.add_experimental_option('useAutomationExtension', False)
	option.add_experimental_option("excludeSwitches", ["enable-automation", "ignore-certificate-errors", "safebrowsing-disable-download-protection", "safebrowsing-disable-auto-update", "disable-client-side-phishing-detection"])

	# Create new Instance of Browser
	global browser
	
	if(browser_path != ''):
		option.binary_location = browser_path
	if(driver_path != ''):
		browser = webdriver.Chrome(executable_path=driver_path, options=option)
	else:
		browser = webdriver.Chrome(options=option)

	browser.delete_all_cookies()
	#browser.set_window_size(800,800)
	#browser.set_window_position(0,0)
	#print(browser.execute_script("return navigator.userAgent;"))
	browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
	browser.execute_script("Object.defineProperty(navigator, 'languages', {get: function() { return ['en-US', 'en']; }, })")
	browser.execute_script("Object.defineProperty(navigator, 'plugins',  { get: function() { return [1, 2, 3, 4, 5]; }, })")
	browser.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.53 Safari/537.36'})
	print(browser.execute_script("return navigator.userAgent;"))
	
	logger.info("browser has been initialized!")


def init_logger(date_fmt, log_level, log_dir, name):
	numeric_level = getattr(logging, log_level.upper(), None)
	if not isinstance(numeric_level, int):
		raise ValueError('Invalid log level: %s' % log_level)

	try:
		now = datetime.datetime.now()	
		logger.setLevel(numeric_level)
		rfh = logging.handlers.TimedRotatingFileHandler(filename=log_dir + '/' + name + '.log', when='midnight')
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
		os.mkdir(data_dir)

	# check log directory
	log_dir = config.get('LOG','LOG_DIR')
	if (not os.path.exists(log_dir)):
		print("creating directory [",log_dir,"]")
		os.mkdir(log_dir)

	init_logger(config.get('LOG','LOG_DATE_FMT'), config.get('LOG','LOG_LEVEL'), config.get('LOG','LOG_DIR'), config.get('LOG','LOG_FILE_PREFIX'))
	
	logger.debug("displaying config values!")

	for section_name in config.sections():
		logger.debug("[SECTION]: %s", section_name)
		logger.debug("  Options:%s", config.options(section_name))
		for name, value in config.items(section_name):
			logger.debug("  %s = %s", name, value)

	logger.info("config file [%s] has been loaded!", file)
	

def main():
	parser = argparse.ArgumentParser(description='Web Scraper')
	parser.add_argument('config_file', type=argparse.FileType('r'),
                    help='configuration file')
	args = parser.parse_args()	
	load_config(config_file)
	init_browser()
	process()
	quit_browser()


def run(yfinance=False, filter_record=False):

	if yfinance is True:
		# Okay actually first run the symbols through yfinance as an easy way to get some data. Then scrape barchart.
		from SEC_Form_4_records_stock_helper_yfinance import yfinance_get_data
		yfinance_get_data()

	from scrape_symbol_helper_generate_input_txt import keep_going_or_not, generate_url_input_txt_file
	keep_going = True

	previous_table_count = 0
	while keep_going:
		keep_going = keep_going_or_not(previous_table_count)

		if keep_going is False:
			print("\nThe last round of updating inserted nothing new in the table. The script thinks there are no more valid symbols to scrape.\n")
			break
		else:
			previous_table_count = keep_going

		# First generate the input txt files of Barchart URLs
		generate_url_input_txt_file()
		# Second run this script to get the data for each stock symbol and update the original table
		main()

	if filter_record is True:
		# Third let's run the script to get all the new SEC Form 4 records and filter and save them to a new table
		from sec_form_4_tables_filter import filter_sec_form_4_records_and_save_to_filtered_table
		filter_sec_form_4_records_and_save_to_filtered_table()

if __name__ == '__main__':
	run(yfinance=True, filter_record=True)
	time.sleep(5)
	run()
	time.sleep(5)
	run()
	time.sleep(5)
	run(filter_record=True)
