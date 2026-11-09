FROM apache/spark:3.5.8
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY spark/requirements.txt /tmp/classic-spark-requirements.txt
COPY wheels/ /tmp/classic-wheels/
RUN cd /tmp/classic-wheels && sha256sum -c SHA256SUMS
RUN python3 -m pip install --no-index --find-links=/tmp/classic-wheels --no-cache-dir -r /tmp/classic-spark-requirements.txt
COPY spark/jars/*.jar /opt/spark/jars/
USER 185
COPY scripts/ /opt/spark-apps/
