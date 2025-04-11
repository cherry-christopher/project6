import json
import vertexai
from vertexai.generative_models import GenerativeModel

region = "us-central1"
model_name = "gemini-2.0-flash-001"
prompt = """Generate a few customer records with the customer's ids and their first and last names. 
Return your answer as json objects with the following schema:
{"id": int, "first_name": string, "last_name": string}
For example:
{"id": 1, "first_name": "Jay", "last_name": "Kreps"},
{"id": 2, "first_name": "Gwen", "last_name": "Shapira"}
"""

def model(dbt, session):
    dbt.config(materialized="table")
    vertexai.init(location=region)
    model = GenerativeModel(model_name)
    resp = model.generate_content([prompt])
    resp_text = resp.text.replace("```json", "").replace("```", "").replace("\n", "")
    print(resp_text)

    json_obj = json.loads(resp_text)
    customer_records = session.createDataFrame(json_obj) 
    return customer_records
