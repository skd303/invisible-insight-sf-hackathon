import pandas as pd
import glob 
from ast import literal_eval
from snowflake.connector.pandas_tools import write_pandas
from Pipeline.proccess_config import C, get_snowflake_conn

ctx = get_snowflake_conn()

#TODO: use llm to consolidate nodes
def main():
    base_dir = "tmp_output\\kg"

    fp_list = glob.glob(base_dir + "/*.parquet")

    outputs = []

    for i, f in enumerate(fp_list):
        try:
            sample_res = pd.read_parquet(f).iloc[0]
            result_raw = sample_res['response'].split("results>")[1]
            arr_str = result_raw.split("</")[0].strip()
            result_arr = literal_eval(arr_str)
            result_df = pd.DataFrame(result_arr)
            for k,v in sample_res.items():
                result_df[k] = v
            
            if len(result_df) >= 1:
                outputs.append(result_df)
        except:
            pass

    output_df = pd.concat(outputs)
    output_df = output_df.rename(columns={"source":"from","target":"to"})
    output_df = output_df.drop_duplicates().reset_index(drop=True)

    write_pandas(ctx, result_df, C.S3_TABLE_NAME, auto_create_table=True)
