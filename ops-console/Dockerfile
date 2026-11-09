FROM python:3.12-slim

WORKDIR /app

COPY ops-console/requirements.txt .

COPY wheels/ /tmp/classic-wheels/
RUN cd /tmp/classic-wheels && sha256sum -c SHA256SUMS
RUN pip install --no-index --find-links=/tmp/classic-wheels --no-cache-dir -r requirements.txt

COPY ops-console/*.py ./
COPY ops-console/templates ./templates

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8090"]
