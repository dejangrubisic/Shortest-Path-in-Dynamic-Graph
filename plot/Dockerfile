FROM python:2.7-slim

# Because || Wheezy and Jessie were recently removed from the mirror network||
RUN echo "deb [check-valid-until=no] http://archive.debian.org/debian jessie-backports main" > /etc/apt/sources.list.d/jessie-backports.list
RUN sed -i '/deb http:\/\/deb.debian.org\/debian jessie-updates main/d' /etc/apt/sources.list
RUN apt-get -o Acquire::Check-Valid-Until=false update

# WORKDIR /ploter
# COPY . /ploter

WORKDIR /plot
COPY . .


# RUN pip install --trusted-host pypi.python.org Flask

EXPOSE 99



# RUN apt-get install nano -y

# RUN pip install --upgrade pip
RUN pip install hdfs
RUN pip install kafka-python

RUN pip install pandas
RUN pip install dash


# CMD ["python", "pr.py"]
CMD ["python", "plot_dash.py"]
