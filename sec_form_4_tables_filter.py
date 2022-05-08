import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

db_suffix = 'EOD'
table_name = 'SEC_form_4_records'
table_suffix = '_filtered'
database_name = f"yhkyxszt_Capewell_{db_suffix}"

db_connection = mysql.connector.connect(host="184.154.190.82",
                                        user="yhkyxszt_capewell_user_updated",
                                        passwd="ENT66%%$43#s",
                                        database=database_name)
db_cursor = db_connection.cursor()

DB_CONNECTION_STRING = 'mysql+pymysql://yhkyxszt_capewell_user_updated:ENT66%%$43#s@184.154.190.82/yhkyxszt_Capewell_EOD'
engine = create_engine(DB_CONNECTION_STRING, pool_recycle=1)

def filter_sec_form_4_records_and_save_to_filtered_table():
    print('\nStarting the filtering process, expect some warning text.\n')
    df1 = pd.read_sql(f'SELECT * FROM `{table_name}` WHERE average_volume != \'0\' and symbol != \'\' ORDER BY id ASC', con=db_connection)

    df2 = pd.read_sql(f'SELECT * FROM `{table_name}{table_suffix}` ', con=db_connection)

    df = df1.merge(df2, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']

    del df['_merge']

    # print(df.tail)

    df["A_or_D_value"] = df['acquired_or_disposed']
    df["triple_criteria_match_count"] = ''
    df["percent_of_A_values_for_date_and_symbol_matches"] = ''
    df.dropna(axis=0, subset=['A_or_D_value'], inplace=True)
    df.reset_index(inplace=True, drop=True)
    # print(df.columns)
    for i in range(df.shape[0]):
        a = df["date"][i]
        b = df["symbol"][i]
        c = df["A_or_D_value"][i]
        df["triple_criteria_match_count"][i] = int(
            df.groupby(['date', 'symbol', 'A_or_D_value']).size().reset_index(name='counts').query(
                'date == @a and symbol == @b and A_or_D_value == @c')["counts"])
    for i in range(df.shape[0]):
        a = df["date"][i]
        b = df["symbol"][i]
        a_per = df.groupby(['date', 'symbol', 'A_or_D_value']).size().reset_index(name='counts').query(
            'date == @a and symbol == @b and A_or_D_value == "A"')["counts"]
        d_per = df.groupby(['date', 'symbol', 'A_or_D_value']).size().reset_index(name='counts').query(
            'date == @a and symbol == @b and A_or_D_value == "D"')["counts"]
        if a_per.empty:
            a_per = 0
        if d_per.empty:
            percent = 1
            df["percent_of_A_values_for_date_and_symbol_matches"][i] = percent
        else:
            percent = int(a_per) / (int(d_per) + int(a_per))
            df["percent_of_A_values_for_date_and_symbol_matches"][i] = round(percent, 5)
    # print(df.head(10))


    # Here is the filtering:
    # 1. Avg trading volume threshold
    df = df.loc[df['average_volume'] > 90000]
    # 2. Market Cap threshold
    df = df.loc[df['market_cap'] > 250000000]
    # 3.  percent_of_A_values_for_date_and_symbol_matches is greater than 80%
    df = df.loc[df['percent_of_A_values_for_date_and_symbol_matches'] > 0.8]
    # 4. A_or_D_value == A, and triple_criteria_match_count >= 2
    df = df.loc[(df['triple_criteria_match_count'] >= 2)]

    del df['percent_of_A_values_for_date_and_symbol_matches']
    del df['A_or_D_value']
    del df['triple_criteria_match_count']

    df2 = df.groupby(['date', 'symbol', 'common_stock_or_derivative', 'acquired_or_disposed']).sum().reset_index()
    df_number_of_shares = df2[['date', 'symbol', 'common_stock_or_derivative', 'acquired_or_disposed']]

    # df.groupby(['date', 'symbol'])["number_of_shares"].sum().reset_index()
    print(df_number_of_shares.columns)
    print(df_number_of_shares)
    try:
        # This saves the first filtered table
        df.to_sql(name=f'{table_name}{table_suffix}', con=engine, if_exists='append', index=False)
        print(len(df), 'records inserted in the first table\n')
    except IntegrityError:
        rows_inserted = 0
        print(rows_inserted, 'records inserted in the first table\n')
        pass

    try:
        # This saves to the second filtered table. The redunancy in the stored values may be useful for doing more analysis in the future.
        df_number_of_shares.to_sql(name=f'{table_name}{table_suffix}_2', con=engine, if_exists='append', index=False)
        rows_inserted = len(df_number_of_shares)
        print(rows_inserted, 'final filtered records inserted in the second table\n')
    except IntegrityError:
        rows_inserted = 0
        print(rows_inserted, 'final filtered records inserted in the second table\n')
        pass

    print('toast.')

if __name__ == '__main__':
    filter_sec_form_4_records_and_save_to_filtered_table()