FROM bde2020/spark-master:2.1.0-hadoop2.8-hive-java8


# Set the working directory to /app
WORKDIR /spark_project

# Copy the current directory contents into the container at /sh_path
# ADD data /sh_path/data
# ADD py /sh_path/py
# ADD app /sh_path/app

COPY . .

# Because || Wheezy and Jessie were recently removed from the mirror network||
RUN echo "deb [check-valid-until=no] http://archive.debian.org/debian jessie-backports main" > /etc/apt/sources.list.d/jessie-backports.list
RUN sed -i '/deb http:\/\/deb.debian.org\/debian jessie-updates main/d' /etc/apt/sources.list
RUN apt-get -o Acquire::Check-Valid-Until=false update

# Make port 80 available to the world outside this container
EXPOSE 85


RUN apt-get install python-pip -y

# RUN pip install pandas
RUN pip install kafka-python
RUN pip install findspark
RUN pip install hdfs

RUN apt update & apt install wget -y
RUN wget http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.4.0/spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar
# RUN apt-get install nano -y
# RUN apt-get install netcat -y

# CMD ["python", "main.py"]

