#FROM ubuntu:16.04
#RUN apt-get update && apt-get -y install wget gnupg2 lsb-core
#RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
#RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -sc)-pgdg main" > /etc/apt/sources.list.d/PostgreSQL.list'


#RUN apt-get update
#RUN apt-get install -y python3 python3-pip


#RUN apt-get install -y postgresql-11


FROM python:3
ENV PYTHONUNBUFFERED 1




RUN mkdir /code
WORKDIR /code
ADD . /code
RUN pip3 install -r requirements.txt
CMD ["python3", "server.py"]





