# Url

Provides URL download functionality

Sample payload:
```json

{
    "type": "url",
    "options": {
        "delay": 1000
    },
    "jobs": [
        {
            "options": {
                "url": "https://google.com/search?q=cat",
                "method": "GET",
                "timeout": 1000
            }
        },
        {
            "options": {
                "url": "https://google.com/search?q=dog",
                "method": "GET",
                "timeout": 1000
            }
        }
    ]
}

```
