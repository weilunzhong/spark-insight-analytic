from ubuntu:14.04

RUN apt-get update && apt-get install -y python python-pip python-dev
RUN pip install --upgrade

COPY . /code
WORKDIR /code
ENV RDB_HOST "192.168.1.124"
ENV RDB_PORT 28015
RUN pip install --index-url https://pypi.vionlabs.com:8080/pypi/ -r requirements.txt
RUN pip install .

WORKDIR /code/insightful/execute
