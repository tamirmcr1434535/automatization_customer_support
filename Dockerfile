FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["functions-framework", "--target", "zendesk_webhook", "--port", "8080"]
