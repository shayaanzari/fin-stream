import json
import os
import requests

LITELLM_KEY = os.getenv("LITELLM_KEY")
API_URL = "http://10.0.0.81:4000/v1/chat/completions" # My Mac Mini

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {LITELLM_KEY}",
}

payload = {
    "model": "nemomoo",
    # OpenRouter allows one of `effort' or 'max_tokens'.
    "reasoning": {
        "effort": "low" # none, low, medium, high
        #"max_tokens": 100
    },
    "messages": [
        {
            "role": "user",
            "content": "Hello! If you can read this, reply with a pun about this futuristic \"hello world\" we are doing."
        }
    ]
}

if __name__ == "__main__":
    try:
        response = requests.post(API_URL, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        print("Response Content:")
        print(data["choices"][0]["message"]["content"])
        print("\nToken Usage Details:")
        print(json.dumps(data.get("usage", {}), indent=2))
    except requests.exceptions.RequestException as e:
        print(f"Error calling LiteLLM API: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print("Response text:", e.response.text)
