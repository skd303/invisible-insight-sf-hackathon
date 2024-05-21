import requests 
from bs4 import BeautifulSoup
import json
import os
import pandas as pd 
import re


class read_edgar:
    def __init__(self):
        self.header = {"User-Agent": "jo boulement jo@gmx.at","Accept-Encoding": "gzip, deflate"}
    
    def output(self, res, trg_format = None):
        return {
            'html': BeautifulSoup(res.content, 'html.parser'), 
            'json': res.json(), 
            'text': str(res.content), 
        }.get(
            trg_format, 
            str(res.content) #Default
        )

    def make_get_request(
        self, 
        target_url,
        output_method = None
    ):
        
        res = requests.get(
            target_url,
            headers=self.header
        )
        return res

    def get_company_facts(self, cik_int):
        cik_str = str(cik_int).zfill(10)
        trg_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json"
        res = requests.get(trg_url,headers=self.header)
        return res.json()

    def get_submissions(self, cik_int):
        cik_str = str(cik_int).zfill(10)
        trg_url = f"https://data.sec.gov/submissions/CIK{cik_str}.json"
        res = requests.get(trg_url,headers=self.header)
        return res.json()

    def make_filing_url(self, cik_int, accessionNumber, primaryDocument):
        accessionNumber_stripped = accessionNumber.replace("-","")
        outputs = f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik_int}/{accessionNumber_stripped}/{primaryDocument}"
        return outputs
    
    @staticmethod
    def get_sic_code_lookup_table():
        res = get_edgar(
        "https://www.sec.gov/corpfin/division-of-corporation-finance-standard-industrial-classification-sic-code-list",
        'content'
        )
        return pd.read_html(res)[0]

    @staticmethod
    def get_company_ticker():
        company_tickers = get_edgar("https://www.sec.gov/files/company_tickers.json",'json')
        company_tickers_df = pd.DataFrame(company_tickers.values())
        return company_tickers_df