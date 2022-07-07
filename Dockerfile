FROM senzing/senzingapi-runtime

ENV DEBIAN_FRONTEND noninteractive
ENV PYTHONUNBUFFERED=1

COPY . /senzing
WORKDIR /senzing

RUN apt update
RUN apt install -y python3 ca-certificates postgresql-client libpq-dev python3-pip python3-icu python3-psycopg2 python3-lxml python3-cryptography
RUN pip3 install --no-cache-dir -q -r ./requirements.txt
