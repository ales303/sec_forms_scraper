import pandas as pd
import mysql.connector
from datetime import datetime, timedelta

two_days_ago = datetime.today().date() - timedelta(1)

db_suffix = 'EOD'
table_name = 'SEC_form_4_records'
database_name = f"yhkyxszt_Capewell_{db_suffix}"

url_prefix = 'https://www.barchart.com/stocks/quotes/'

def generate_url_input_txt_file():
    db_connection = mysql.connector.connect(host="184.154.190.82",
                                            user="yhkyxszt_capewell_user_updated",
                                            passwd="ENT66%%$43#s",
                                            database=database_name)
    db_cursor = db_connection.cursor()
    df = pd.read_sql(f'SELECT symbol, date FROM `{table_name}` WHERE average_volume = \'0\' AND symbol != \'NONE\' AND symbol != \'\'  '
                     f'AND symbol NOT LIKE \'%.%\' AND symbol NOT LIKE \'%,%\' AND symbol NOT LIKE \'%/%\' AND symbol NOT LIKE \'%:%\' '
                     f'ORDER BY id DESC LIMIT 140 ', con=db_connection)

    df.drop(df[df.date < two_days_ago].index, inplace=True)
    del df['date']

    content = set()

    for stock_symbol in df['symbol']:
        url = url_prefix + stock_symbol
        content.add(url)

    with open("sec_symbols_to_scrape.txt", "w") as output:
        for link in content:
            output.write(str(link) + '\n')

    print('Input URL text file created.\n')

def update_sec_form_4_record_with_stock_data(symbol, market_cap, average_volume):
    db_connection = mysql.connector.connect(host="184.154.190.82",
                                            user="yhkyxszt_capewell_user_updated",
                                            passwd="ENT66%%$43#s",
                                            database=database_name)
    db_cursor = db_connection.cursor()
    sql = f"UPDATE `{table_name}` SET market_cap = \'{market_cap}\', average_volume = \'{average_volume}\' " \
          f"WHERE symbol = \'{symbol}\' AND average_volume = \'0\'"
    print(f"Currently saving {symbol} to table {table_name}")
    db_cursor.execute(sql)
    db_connection.commit()

def keep_going_or_not(previous_table_count):
    db_connection = mysql.connector.connect(host="184.154.190.82",
                                            user="yhkyxszt_capewell_user_updated",
                                            passwd="ENT66%%$43#s",
                                            database=database_name)
    db_cursor = db_connection.cursor()
    df = pd.read_sql(f'SELECT symbol, date FROM `{table_name}` WHERE average_volume = \'0\' AND symbol != \'NONE\' AND symbol != \'\'  '
                     f'AND symbol NOT LIKE \'%.%\' AND symbol NOT LIKE \'%,%\' AND symbol NOT LIKE \'%/%\' AND symbol NOT LIKE \'%:%\' '
                     f'ORDER BY id ASC ', con=db_connection)

    df.drop(df[df.date < two_days_ago].index, inplace=True)

    if previous_table_count == len(df):
        return False
    else:
        print(f'\nThere are {len(df)} stock symbols remaining in the table to scrape.\n')
        return len(df)


if __name__ == '__main__':
    generate_url_input_txt_file()