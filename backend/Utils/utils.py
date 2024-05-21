from together import Together
import os 
from dotenv import load_dotenv
load_dotenv()

# LLM_NAME = 'Snowflake/snowflake-arctic-instruct'

def get_completion(prompt, model_name = 'meta-llama/Llama-3-8b-chat-hf'):
    together_client = Together(api_key=os.environ["TOGETHER_API_KEY"])

    response = together_client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
    )

    response_str = response.choices[0].message.content
    return response_str

def make_batches(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

