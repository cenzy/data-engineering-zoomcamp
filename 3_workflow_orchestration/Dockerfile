FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

RUN mkdir -p /opt/prefect/data/
RUN mkdir -p /opt/prefect/flows/

COPY  etl_gcs_to_bq.py /opt/prefect/flows
COPY  etl_web_to_gcs.py /opt/prefect/flows
COPY  parameter_flow.py /opt/prefect/flows
COPY  ingest_data.py /opt/prefect/flows
COPY data /opt/prefect/data