from dataclasses import dataclass
import streamlit as st
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List
import snowflake.connector
load_dotenv()

@dataclass
class DataSources:
    COMPANY_INDEX_TABLE: str
    S1_TABLE_NAME:str 
    S2_TABLE_NAME:str

C = DataSources(
    COMPANY_INDEX_TABLE = 'COMPANY_INDEX',
    S1_TABLE_NAME='S1_FILINGS',
    S2_TABLE_NAME='S2A_LLM'
)

def get_snowflake_conn():
    ctx = snowflake.connector.connect(
        user=os.environ['SF_USERNAME'],
        password=os.environ['SF_PASSWORD'],
        account=os.environ['SF_ACCOUNT'],
        database=os.environ['SF_DATABASE'],
        schema=os.environ['SF_SCHEMA']
    )
    return ctx
