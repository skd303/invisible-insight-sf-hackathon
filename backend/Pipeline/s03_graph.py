import glob 
import pandas as pd 
import os 
from dotenv import load_dotenv 
from Utils.utils import get_completion
load_dotenv()



system_prompt = """You are a data scientist working for a company that is building a graph database. Your task is to extract information from an SEC filing and convert it into a graph database. Provide a set of Nodes in the form: [{"source":ENTITY_ID_1, "source_type":TYPE, "target":ENTITY_ID_2, "target_type":TYPE, "relationship": RELATIONSHIP},...]. If you can't pair a relationship with a pair of nodes don't add it. The only allowed relationship types are: ["CUSTOMERS", "SUPPLIERS_OR_PARTNERS", "COMPETITORS"]. The target and source nodes should be of type "COMPANY". Print out the results in <results> tag using JSON format.

Here is an example input, output pair:
<main_entity> Microsoft </<main_entity>. <document> We compete to provide enterprise-wide computing solutions and point solutions with numerous commercial software vendors that offer solutions and middleware technology platforms, software applications for connectivity (both Internet and intranet), security, hosting, database, and e-business servers. Commercial competitors for our server applications for PC-based distributed client-server environments include CA Technologies, IBM, and Oracle. </document>

<result>
[
    {"source":"Microsoft","source_type":"COMPANY","target":"CA Technologies","target_type":"COMPANY","relationship":"COMPETITORS"},
    {"source":"Microsoft","source_type":"COMPANY","target":"IBM","target_type":"COMPANY","relationship":"COMPETITORS"},
    {"source":"Microsoft","source_type":"COMPANY","target":"Oracle","target_type":"COMPANY","relationship":"COMPETITORS"},
]
</result>
"""


def main():
    
    input_df = pd.read_parquet('tmp_data\\s02_data.parquet')
    
    for i, row in input_df.iterrows():
        entity_id_str = f"{row['companyname']} (stock ticker: {row['primarysymbol']}"
        main_entity = entity_id_str
        text = row['text']
        user_prompt = f"""Now perform the task.
        <main_entity>
        {main_entity}
        </main_entity>

        Here is the document:
        <document>
        {text}
        </document>"""
        prompt = system_prompt + user_prompt
            
        try: 
            
            res1 = get_completion(prompt)
            print(res1)
            outputs = row.copy()
            outputs['response'] = res1
            pd.DataFrame([outputs]).to_parquet(f"tmp_data\\kg\\{row['entity_id']}_{i}.parquet")
            
        
        except Exception as e:
            print(row['entity_id'],e)
        

