import pandas as pd
import pymssql

pd.set_option('display.expand_frame_repr', False)
"""
Build a data pipeline that computes and stores performance metrics of various funds, given the price per share, and dividend per share attribute. Store in SQL database. Computed metrics should include;

Base monthly performance, Life to date performance, year to date performance


Each fund commences operations with an initial fund price of 1000.
Prices listed in the supporting files represent the closing prices for each respective month.

1. Build a data model that represents fund metadata as well as performance metrics for each month.

2. Build a data pipeline that computes perfomance metrics for each month and store them in the database.
"""


# Load in fund metadata
def load_metadata():
    df = pd.read_csv('fund_metadata.txt', delimiter = '|')
    df['Fund ID'] = df['Fund ID'].str.replace('fund','').astype(int)
    return df
    
#load and transform fund data
def load_funddata():
    fund1 = pd.read_csv('fund1.csv')
    fund2 = pd.read_json('fund2.json')

    # merging funds into a singular df
    fund1['fund_id'] = 1
    fund2['fund_id'] = 2 

    combined_funds = pd.concat([fund1,fund2])

    #as we're reviewing by month, we'll need to create date objects
    combined_funds['date'] = combined_funds.apply(lambda row: pd.Timestamp(year=row['Year'].astype(int), month=row['Month'].astype(int), day=1), axis=1)
    
    
    return combined_funds

# computing Performance metrics for each month
def metrics(df):
    
    #METRIC 1 - Base Monthly Performance (groupby fund_id and grab the last price per share)
    df = df.sort_values(by=['fund_id', 'date'])
    
    df['past_price'] = df.groupby('fund_id')['Price Per Share'].shift()

    df.loc[df['past_price'].isnull(), 'past_price'] = 1000

    df['bmf'] = ((df['Price Per Share'] - df['Dividend Per Share']) / df['past_price']) -1
        
    df['Base Monthly Performance (%)'] = round(df['bmf'] * 100, 2)    
    
    #METRIC 2 - Life To Date Performance
    
    df['cumulative_return_factor'] = df['bmf'] + 1 
    df['LTD Performance (%)'] = df.groupby('fund_id')['cumulative_return_factor'].cumprod() - 1
    
    df['LTD Performance (%)'] = round(df['LTD Performance (%)'] * 100,2)
    
    #METRIC 3 - Year To Date Performance 
    df['ytd'] = df.groupby(['fund_id', 'Year'])['cumulative_return_factor'].cumprod() - 1
    df['YTD Performance (%)'] = round(df['ytd'] * 100,2)

    df = df.where(pd.notna(df), 0)
    
    return df[['fund_id','Year','Month','Price Per Share','Dividend Per Share','Base Monthly Performance (%)','LTD Performance (%)','YTD Performance (%)']]

def metadata_create_insert(data,server,username,password,db):
    conn = pymssql.connect(server=server, user=username, password=password, database=db)
    cursor = conn.cursor()
    
    create_query_fund_metadata = '''
    CREATE TABLE fund_metadata (
        [Fund ID] INTEGER PRIMARY KEY,
        [Fund Name] TEXT,
        [Manager] TEXT,
        [Inception Date] DATE,
        [Base Currency] CHAR(3),
        [Investment Strategy] TEXT,
        [Fund Size (in millions)] INTEGER,
        [Initial Price] INTEGER
    )
    '''
    cursor.execute(create_query_fund_metadata)
    conn.commit()
    
    row = [tuple(x) for x in data.values]
    
    insert_query_metadata = '''
            INSERT INTO fund_metadata ([Fund ID], [Fund Name], [Manager], [Inception Date], [Base Currency], [Investment Strategy], [Fund Size (in millions)], [Initial Price])
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''

    cursor.executemany(insert_query_metadata, row)
    conn.commit()
    cursor.close()
    conn.close()
     
def fund_db_create_insert(data,server,username,password,db):
    conn = pymssql.connect(server=server, user=username, password=password, database=db)
    cursor = conn.cursor()

    # creating our sql table
    create_query_fund_data = '''
    CREATE TABLE performance_metrics (
        [fund_id] FLOAT,
        [Year] INTEGER,
        [Month] FLOAT,
        [Price Per Share] FLOAT,
        [Dividend Per Share] FLOAT,
        [Base Monthly Performance (%)] FLOAT,
        [LTD Performance (%)] FLOAT,
        [YTD Performance (%)] FLOAT
    )
    '''
    cursor.execute(create_query_fund_data)
    conn.commit()
    
    # Now to insert data into the table with a basic insert query
    # create the isolated data tuples for entry
 
    row = [tuple(x) for x in data.values]
    
    insert_query = '''
            INSERT INTO performance_metrics ([fund_id], [Year], [Month], [Price Per Share], [Dividend Per Share], [Base Monthly Performance (%)], [LTD Performance (%)], [YTD Performance (%)])
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''
 
    cursor.executemany(insert_query, row)
    conn.commit()
    
    cursor.close()
    conn.close()
    
if __name__ == '__main__':

    metadata_df=load_metadata()

    fund_df = load_funddata()
    output = metrics(fund_df)

    server = 'localhost'
    username = 'sa'
    password =  'VeryrealPassw0rd!'
    db = 'master'

    metadata_create_insert(metadata_df,server,username,password,db)
    
    fund_db_create_insert(output,server,username,password,db)
    
    
