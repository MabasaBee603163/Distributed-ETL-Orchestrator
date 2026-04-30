FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ETL_NO_GUI=1 \
    DO_NOT_TRACK=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN addgroup --system app && adduser --system --ingroup app app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY distributed_etl /app/distributed_etl
COPY main.py /app/main.py

USER app

# Default container behavior: headless CLI (Tk GUI won't work in most containers).
# Prefer `docker/Dockerfile.worker` for long-running Prefect workers.
CMD ["python", "main.py", "run-csv"]

