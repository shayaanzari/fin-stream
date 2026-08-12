import os

from openai import OpenAI


client = OpenAI(
    base_url=os.getenv("LITELLM_BASE_URL", "http://10.0.0.81:4000/v1"),
    api_key=os.getenv("LITELLM_KEY", ""),
)

response = client.chat.completions.create(
    model=os.getenv("LLM_MODEL", "openrouter/openrouter/free"),
    messages=[
        {
            "role": "user",
            "content": 'Hello! If you can read this, reply with a pun about this futuristic "hello world" we are doing.',
        }
    ],
    # Uncomment optional request features as needed:
    # reasoning={"effort": "low"},  # none, low, medium, high
    # temperature=0.0,
    # max_tokens=100,
)

print(response.choices[0].message.content)
