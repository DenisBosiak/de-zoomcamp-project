FROM prefecthq/prefect:2.7.7-python3.9

COPY requirements.txt .

RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
COPY .kaggle /root/.kaggle

RUN chmod 600 /root/.kaggle/kaggle.json
RUN mkdir -p /opt/prefect/dataset
RUN mkdir -p /opt/prefect/upload