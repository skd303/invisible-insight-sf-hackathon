from dataclasses import dataclass
import snowflake.connector
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List
load_dotenv()

@dataclass
class DataSources:
    COMPANY_INDEX_TABLE: str
    S1_TABLE_NAME:str 
    S2_TABLE_NAME:str
    S3_TABLE_NAME:str

C = DataSources(
    COMPANY_INDEX_TABLE = 'COMPANY_INDEX',
    S1_TABLE_NAME='S1_FILINGS',
    S2_TABLE_NAME='S2A_LLM',
    S3_TABLE_NAME='EDGAR_COMPETITORS_GRAPH'
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

###########################################
# API Data Model
###########################################
class PromptAndMeta(BaseModel):
    prompt: str 
    entity_id: str
    ticker: str 
    cik: str 
    accepted_date: str
    src_document: str


class BatchCompletionJob(BaseModel):
    parent_task_id: int
    prompts_and_metadata: List[PromptAndMeta]
    write_table_name:str
