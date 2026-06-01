FROM python:3.12-alpine

WORKDIR /app

RUN pip install requests websocket-client

COPY tiktok_agent.py /app/tiktok_agent.py

CMD ["python", "-u", "/app/tiktok_agent.py"]
