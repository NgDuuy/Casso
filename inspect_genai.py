from google.genai import types
import json
schema = types.Content.model_json_schema()
print(json.dumps(schema, indent=2)[:2000])
