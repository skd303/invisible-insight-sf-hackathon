from bs4 import BeautifulSoup
import pandas as pd
import requests 
import numpy as np 
from read.read_edgar import read_edgar 
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from dotenv import load_dotenv
import datetime
load_dotenv()
import os
import time
from Pipeline.proccess_config import C, get_snowflake_conn
import logging
logging.basicConfig(filename='s01_ingest.log',level=logging.INFO,encoding='utf-8')

E = read_edgar()
ctx = get_snowflake_conn()
local_file_path = "entity_id_index\\index-table-2024-05-19.parquet"
write_table_name = C.S1_TABLE_NAME

######################################################################################################
# Schema
######################################################################################################
# input
code_name_col = 'Code'
# output
primary_id_cols = [
    'entity_id',
    'cik',
    'primarysymbol',
    'companyname',
]
primary_date_col = 'accepted_dt'

index_df_cols = ['entity_id', 'cik', 'primarysymbol', 'companyname', 'addl_metadata',
        'company_facts_url', 'company_submissions_url']

company_cols_trimmed = ['entity_id', 'cik', 'primarysymbol', 'companyname', 
        'company_facts_url', 'company_submissions_url']

######################################################################################################
# Helper Functions
######################################################################################################

def parse_method(c, m = None):
    #'html'
    return BeautifulSoup(c.content,'html.parser').text

def screen(
    input_df:pd.DataFrame = None, 
    date_col: str = None,
    min_date = datetime.datetime(2020,6,1)
)->pd.DataFrame:
    offering_doc_form = 'S-1'
    addl_trg_forms = [
        '20',
        '10',
        '10',
        '8',
        # 'DEF'
    ]
    output_df_0 = input_df.loc[
        (input_df['form'].str.contains('|'.join(addl_trg_forms))) & 
        (input_df[date_col]>=min_date) 
    ] # All recent: 8k, 10q, 10k
    output_df_1 = input_df.loc[input_df['form']==offering_doc_form] # S1 filing
    output_df = pd.concat([output_df_0, output_df_1])
    return output_df.reset_index(drop=True)

######################################################################################################
# Main
######################################################################################################

def main():
    # Change Event
    existing_tables = pd.read_sql('SHOW TABLES',ctx)['name'].tolist()
    already_proccessed = []
    if write_table_name in existing_tables:
        already_proccessed = pd.read_sql(f"SELECT DISTINCT 'filing_src_url' FROM {write_table_name}", ctx)['filing_src_url'].tolist()

    
    company_index = pd.read_parquet(local_file_path,columns=index_df_cols)

    if C.COMPANY_INDEX_TABLE not in existing_tables: #create if does NOT exist
        write_pandas(ctx, company_index, C.COMPANY_INDEX_TABLE,auto_create_table=True)
    
    # Main Proccess
    for _, company in company_index.iterrows(): 

        print(f"starting: {company['companyname']}")
        
        company_submissions = requests.get(company['company_submissions_url'],headers=E.header).json()
        filing_df = pd.DataFrame(company_submissions['filings']['recent'])
        filing_df['url'] = [E.make_filing_url(company['cik'], x['accessionNumber'],x['primaryDocument']) for _,x in filing_df.iterrows()]
        filing_df[primary_date_col] = pd.to_datetime(filing_df['acceptanceDateTime'].apply(lambda x: x.split(".")[0]))
        filing_df = filing_df.sort_values(primary_date_col,ascending=True).reset_index(drop=True)
        filing_df['filing_src_url'] = filing_df['url'].apply(lambda x: x.replace('ix?doc=','')  if 'ix?' in x else x)
        
        # Change Event
        if len(already_proccessed) > 0:
            filing_df = filing_df.loc[~filing_df['filing_src_url'].isin(already_proccessed)]
        
        filing_df_trimmed = screen(filing_df, date_col=primary_date_col)

        
        for _, filing_metadata in filing_df_trimmed.iterrows():
            try:
                target_url = filing_metadata['filing_src_url']
                res = requests.get(target_url, headers = E.header)

                print(f"success: {target_url}")
                logging.info(f"success: {target_url}")

                time.sleep(.125) #ensure stay below rate limit of 10 requests per second
                response_text = parse_method(res, 'html')
                
                company_trimmed = {k:v for k,v in company.items() if k in company_cols_trimmed}
                result_df = pd.DataFrame([{
                    **company_trimmed, 
                    **filing_metadata,
                    **{'text':response_text}
                }])
        
                write_pandas(ctx, result_df, write_table_name, auto_create_table=True)
            except Exception as e:
                print(e)
                logging.info(f"Error for {target_url}: {e}")
            
