FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pytest.ini .
COPY app ./app
COPY migrations ./migrations
COPY tests ./tests

# default command is the API; compose overrides it for worker / webhook worker / tests
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
