import os
from dotenv import load_dotenv
load_dotenv()
from Pipeline.proccess_config import get_snowflake_conn, C
import pandas as pd
from langchain_text_splitters import RecursiveCharacterTextSplitter
from dotenv import load_dotenv
load_dotenv()

ctx = get_snowflake_conn()


cols_trimmed = ['entity_id','cik','primarysymbol','companyname','filingDate','filing_src_url']

text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
    model_name="gpt-4-turbo-preview",
    chunk_size=900,
    chunk_overlap=0,
    separators= ['ITEM [0-9]','\n\n','\n','. ',' ']
)

def upper_word_search(text_str, min_upper_words = 3):
    
    sents = text_str.split(".")
    upper_case_word_count = 0
    for sent in sents:
        words = [w.strip() for w in sent.split(" ")]
        upper_case_words = [w for i, w in enumerate(words) if i > 1 and len(w) > 2 and w[0].isupper()]
        upper_case_word_count += len(upper_case_words)

    return upper_case_word_count

def main():

    df = pd.read_sql(f'''SELECT * FROM {C.S1_TABLE_NAME}''',ctx)
    df['text_stripped'] = df['text'].apply(lambda x: " ".join(x.split()))
    df['word_count'] = df['text_stripped'].apply(lambda x: len(x.split()))
    df['filingDate'] = pd.to_datetime(df['filingDate'])
    df_filt = df.loc[df['form'].str.contains('10')].copy()

    flat_list = []

    for _, r in df_filt.iterrows():
        splits = text_splitter.split_text(r['text'])
        splits_1 = [" ".join(x.split()) for x in splits if len(x.strip()) >1 ]
        chunk_df = pd.DataFrame([{'chunk_id':i, 'text':x} for i, x in enumerate(splits_1)])
        for c in cols_trimmed:
            chunk_df[c] = r[c]
        
        flat_list.append(chunk_df)

    chunk_df_one = pd.concat(flat_list)
    chunk_df_one['flag_1'] = chunk_df_one['text'].apply(upper_word_search)
    chunk_df_one['flag_2'] = chunk_df_one['text'].apply(lambda x: 1 if "compet" in x.lower() else 0)

    output_df = chunk_df_one.loc[
        (chunk_df_one['flag_1']>2) & 
        (chunk_df_one['flag_2']==1) &
        (chunk_df_one['filingDate'].dt.year>=2023)
        ]
    
    output_df.to_parquet('tmp_data//s02_data.parquet')
 
    return 200
