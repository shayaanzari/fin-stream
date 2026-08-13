curl http://10.0.0.81:4000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${LITELLM_KEY}" \
  -d '{
    "model": "nemomoo",
    "reasoning": {
      "effort": "none"
    },
    "messages": [
      {
        "role": "user",
        "content": "Hello! Reply with a fitting pun about \"hello world\" if you can read this."
      }
    ]
  }'


