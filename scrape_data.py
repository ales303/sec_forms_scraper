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
import numpy as np
from dateutil import parser
from shutil import move
import random
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

DB_CONNECTION_STRING = 'mysql+pymysql://yhkyxszt_capewell_user_updated:ENT66%%$43#s@184.154.190.82/yhkyxszt_Capewell_EOD'
engine = create_engine(DB_CONNECTION_STRING, pool_recycle=1)

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

def init_global():
	global _proc_date
	_proc_date = ""

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

def explode(df, lst_cols, length, fill_value=''):
	# make sure `lst_cols` is a list
	if lst_cols and not isinstance(lst_cols, list):
		lst_cols = [lst_cols]
	# all columns except `lst_cols`
	idx_cols = df.columns.difference(lst_cols)
    
	# calculate lengths of lists
	lens = df[lst_cols[0]].str.len()

	# normalize lst_cols
	for x in lst_cols:
		arr = df.iloc[0][x]
		#print(arr[:length])
		df.at[0,x] = arr[:length]
    
	if (lens > 0).all():
		# ALL lists in cells aren't empty
		return pd.DataFrame({
			col:np.repeat(df[col].values, df[lst_cols[0]].str.len())
			for col in idx_cols
		}).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
		.loc[:, df.columns]
	else:
		# at least one list in cells is empty
		return pd.DataFrame({
			col:np.repeat(df[col].values, df[lst_cols[0]].str.len())
			for col in idx_cols
		}).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
		.append(df.loc[lens==0, idx_cols]).fillna(fill_value) \
		.loc[:, df.columns]

def parse(url, soup):
	lookup_data = {}
	dict = {}

	#print(soup)
	table = soup.find_all('div', attrs={'class':'divTr'})
	
	#rows = row.find_all('div', attrs={'class':'divTr'})
	for row in table:
		#print(row)
		td = row.find_all('div', attrs={'class':'divTd'})
		
		if(len(td) > 0):
			try:
				key = td[0].text.strip()
			except:
				key = ""		

			#if(key == "Derivative Transaction" or key == "Non Derivative Holding"):
			#	break
			if(key == "Non Derivative Transaction"):
				flag = "Non_Derivative_Transaction"
			if(key == "Non Derivative Holding"):
				flag = "Non_Derivative_Holding"
			if(key == "Derivative Transaction"):
				flag = "Derivative_Transaction"
			if(key == "Derivative Holding"):
				flag = "Derivative_Holding"

			if(key == "Footnote Id"):
				continue
			if(key == "@attributes Id" and prev_key == "Transaction Shares" and orig_key != "Value"):
				key = "Transaction Price Per Share"
			if(key == "@attributes Id" and prev_key == "Transaction Shares" and orig_key != "@attributes Id"):
				key = "Transaction Price Per Share"
			if(key == "Value Owned Following Transaction"):
				key = "Shares Owned Following Transaction"

			orig_key = key
			if(key == "Value"):
				key = prev_key
			prev_key = key


		if(len(td) > 1):
			try:
				val = td[1].text.strip()
			except:
				val = ""	

			if(key == "Transaction Shares"):
				val = flag + "~" + val

			#print(key, "=", val)
			if key in lookup_data:
				lookup_data[key] = lookup_data[key] + "|" + val
			else:
				lookup_data[key] = val

	#print(lookup_data)

	# convert list columns to list
	fields = config.get('OUTPUT', 'list_columns')
	list_columns = [x.strip() for x in fields.split(',')]

	for x in config.get('OUTPUT','fields').split(','):
		key = x.strip()
		logger.debug("[%s]", key)
		type = lookup(key,'type')
		#print("type: ", "=", type)
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
		
		if(type == "url"):
			val = url

		if(type == "fixed"):
			val = lookup(key,'value')
			
		if(type == "lookup"):
			try:
				val = lookup_data[lookup(key,'value')]
				if(key in list_columns):
					fields = val.split('|')
					#print(fields)
					val = fields
			except:
				val = ""

		if(type == "html"):
		
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

			remove_char = [x.strip() for x in lookup(key,'remove_char').split(',')]
			#print(remove_char)
			#print("|".join(remove_char))
			val = re.sub("|".join(remove_char), "", val)
			
			# remove string
			val = re.sub(lookup(key,'remove_string'),'',val)
			
			# remove non unicode characters
			val = re.sub(r'[^\x00-\x7F]','', val)


		logger.debug("%s = %s", key, val)
		#print(key, "=", val)
		dict[key]=val
	
	return dict

def scrape(url):
	rows = []
	dict = {}

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

	print("Scraping data from: ", url)
	logger.info("Scraping data from: %s", url)

	for count in range(1,retry_count+1):
		try:
			# userAgent = random.choice(user_agent_list)
			# browser.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": userAgent})
			print(browser.execute_script("return navigator.userAgent;"))
			# open url in browser
			browser.get(url)
		except Exception as e:
			logger.debug("failed to open url in browser (%s)", e)
			exit(1)

		# sleep to load the URL
		time.sleep(wait_time)

		# Parse HTML and save to BeautifulSoup object
		soup = BeautifulSoup(browser.page_source, "html.parser")

		published = soup.find('abbr', attrs={'class':'published'})
		print(published)
		
		# exit loop if table is found
		if(published != None):
			break

	dict = parse(url, soup)

	rows.append(dict)

	return rows

def process():

	global _proc_date

	data = []
	lines = []
	
	now = datetime.datetime.now()
	curr_date = now.strftime('%Y-%m-%d')
	logger.info("current date [%s]", curr_date)
	file = config.get('INPUT', 'input_list')
	logger.info("loading input list [%s]", file)

	if(_proc_date == ""):
		_proc_date = curr_date

	print("Proc Date: ", _proc_date)
	print("Curr Date: ", curr_date)

	# load input file
	try:
		fp = open(file)
	except Exception as e:
		print("exception: ", e)
		logger.debug("exception (%s)", e)
		exit(1)

	# convert header columns to list
	fields = config.get('OUTPUT', 'fields')
	headers = [x.strip() for x in fields.split(',')]
	#print("Column Headers: ", headers)

	# convert list columns to list
	fields = config.get('OUTPUT', 'list_columns')
	list_columns = [x.strip() for x in fields.split(',')]
	#print("List Columns: ", list_columns)

	# read lines
	for line in fp:
		lines.append(line)
	fp.close()
	
	# remove duplicates
	lines = list(dict.fromkeys(lines))

	# read lines
	for line in lines:
		#print(line)
		(date, url) = line.strip().split(config.get('INPUT', 'delimiter'))
		
		# skip procesing
		if(_proc_date != "all" and _proc_date != date):
			continue
		
		print("Processing Date: ", date)
		#print("Processing URL: ", url)
		data = scrape(url)
		print(data)

		# Create DataFrame from scraped data
		df = pd.DataFrame(data, columns=headers)

		# check if need to explode (split column list into multiple rows)
		ts = df.iloc[0][list_columns[0]] 
		if(isinstance(ts, list)):
			# create multiple rows if columns contains list
			df = explode(df, lst_cols=list_columns, length=len(ts))

		# split column TS
		try:
			df[['TYPE', 'SHARES']] = df.TS.str.split("~", expand=True)
		except ValueError as exp:
			print('The returned URL data has a blank field, view the URL to see more', exp)
			df['TYPE'] = ""
			df['SHARES'] = ""
			df['TS'] = ""
			df['TPPP'] = ""
			df['TADC'] = ""
			df['SOFT'] = ""

		print(df)

		column_names = ["date", "symbol", "number_of_shares", "acquired_or_disposed", "transaction_share_price",
						"shares_owned_following_transaction", "url"]

		df_modified = pd.DataFrame(columns=column_names)
		df_modified['date'] = df['PUBLISHED']
		df_modified['symbol'] = df['SYMBOL']
		df_modified['number_of_shares'] = df['SHARES']
		df_modified['acquired_or_disposed'] = df['TADC']
		df_modified['transaction_share_price'] = df['TPPP']
		df_modified['shares_owned_following_transaction'] = df['SOFT']
		df_modified['url'] = df['URL']
		df_modified['common_stock_or_derivative'] = df['TYPE']

		symbol_string = df_modified['symbol']

		print('\n', df_modified, '\n')

		try:
		   df_modified.to_sql(name='SEC_form_4_records', con=engine, if_exists='append', index=False)
		   rows_inserted = len(df)
		   print(rows_inserted, 'rows inserted\n')
		except IntegrityError:
		   rows_inserted = 0
		   print(rows_inserted, 'rows inserted\n')
		   pass


		logger.info(f"Finished updating table for symbol {symbol_string}")



	# move file to archive after processing
	archive_file = config.get('DEFAULT', 'data_dir') +'/'+  curr_date + "." + os.path.basename(file)
	try:
		move(file, archive_file)
	except Exception as e:
		print("exception: ", e)
		logger.debug("exception (%s)", e)
		exit(1)

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
		ua = UserAgent()
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

def setProcDate(x):
	global _proc_date
	print("setting processing date: ", x)
	_proc_date = x

def main():
	init_global()
	parser = argparse.ArgumentParser(description='Scrape Data v1.0')
	parser.add_argument('config_file', type=argparse.FileType('r'),
                    help='configuration file')
	parser.add_argument('--setProcDate', type=setProcDate,
                    help='set processing date [yyyy-mm-dd|all]')
	args = parser.parse_args()					
	load_config(config_file)
	init_browser()	
	process()
	quit_browser()


if __name__ == '__main__':
	main()

