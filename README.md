# Openrelik worker N8N

## Description
Openrelik Worker that allows to trigger a n8n workflow by sending files via post to a configured n8n webhook. 

## Deploy
Add the below configuration to the OpenRelik docker-compose.yml file.

```
openrelik-worker-n8n:
    container_name: openrelik-worker-n8n
    image: ghcr.io/openrelik/openrelik-worker-n8n:latest
    restart: always
    environment:
      - REDIS_URL=redis://openrelik-redis:6379
      - OPENRELIK_PYDEBUG=0
    volumes:
      - ./data:/usr/share/openrelik/data
    command: "celery --app=src.app worker --task-events --concurrency=4 --loglevel=INFO -Q openrelik-worker-n8n"
    # ports:
      # - 5678:5678 # For debugging purposes.
```

## Test
```
uv sync --group test
uv run pytest -s --cov=.
```
