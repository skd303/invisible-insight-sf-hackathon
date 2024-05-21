from together import Together
import os 
from dotenv import load_dotenv
load_dotenv()

# LLM_NAME = 'Snowflake/snowflake-arctic-instruct'

def get_completion(prompt, model_name):
    together_client = Together(api_key=os.environ["TOGETHER_API_KEY"])

    response = together_client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.choices[0].message.content