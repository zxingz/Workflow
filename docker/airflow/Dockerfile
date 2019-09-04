FROM python:3

RUN apt-get update && apt-get install git -y

WORKDIR /airflow

# Set airflow variables
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes
ENV AIRFLOW_HOME=/airflow

# get the req file
COPY requirements.txt /requirements.txt

# install python libraries
RUN pip install -r  /requirements.txt
