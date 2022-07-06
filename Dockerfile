FROM ubuntu:20.04
ENV DEBIAN_FRONTEND noninteractive
ENV PYTHONUNBUFFERED=1
ENV NONINTERACTIVE=1

# Configure some docker defaults:
ENV SENZING_ROOT /opt/senzing/g2
ENV PYTHONPATH $SENZING_ROOT/python/senzing:$SENZING_ROOT/python
ENV LD_LIBRARY_PATH $SENZING_ROOT/lib:$SENZING_ROOT/lib/jre/lib/server:$SENZING_ROOT/lib/debian:$LD_LIBRARY_PATH
ENV CLASSPATH $SENZING_ROOT/lib/g2rst.jar:$SENZING_ROOT/lib/g2.jar:$CLASSPATH
ENV SENZING_ACCEPT_EULA "i_accept_the_senzing_eula"

COPY . /senzing
WORKDIR /senzing

RUN rm -rf ./senzingapi/

RUN apt update && apt install -y wget apt-utils

RUN wget https://senzing-production-apt.s3.amazonaws.com/senzingrepo_1.0.0-1_amd64.deb

RUN apt install -y apt-transport-https ca-certificates postgresql-client libpq-dev curl jq\
    python3-pip python3-icu python3-psycopg2 \
    python3-lxml python3-crypto gnupg2
RUN apt install -y ./senzingrepo_1.0.0-1_amd64.deb
RUN apt update
RUN apt -y install senzingapi

RUN pip3 install --no-cache-dir -q -r ./requirements.txt

RUN mkdir -p /etc/opt/senzing
RUN cp -R /opt/senzing/g2/resources/templates/* /etc/opt/senzing/
