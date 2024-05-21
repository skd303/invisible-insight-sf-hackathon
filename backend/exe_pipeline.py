import importlib
import sys
import os 
from dotenv import load_dotenv
import datetime
load_dotenv()

def main():
    package_name = "Pipeline"
    steps = [
       "Pipeline.s01_ingest",
       "Pipeline.s02_chunk",
       "Pipeline.s03_graph",
       "Pipeline.s03b_graph"
    ]
    
    for step in steps:
        print(f"starting: {step}, time: {datetime.datetime.now()}")
        mod = importlib.import_module(step)
        sys.path.insert(0,package_name)
        mod.main()

if __name__ == "__main__":
    main()