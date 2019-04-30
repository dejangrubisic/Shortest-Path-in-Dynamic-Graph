FROM python:2.7-slim

# Because || Wheezy and Jessie were recently removed from the mirror network||
RUN echo "deb [check-valid-until=no] http://archive.debian.org/debian jessie-backports main" > /etc/apt/sources.list.d/jessie-backports.list
RUN sed -i '/deb http:\/\/deb.debian.org\/debian jessie-updates main/d' /etc/apt/sources.list
RUN apt-get -o Acquire::Check-Valid-Until=false update

# WORKDIR /gen

# ADD . /gen
WORKDIR /gen
COPY . .

# RUN pip install --upgrade pip
RUN pip install hdfs
RUN pip install kafka-python


# CMD ["python", "gen.py"]
CMD ["python", "pr.py"]