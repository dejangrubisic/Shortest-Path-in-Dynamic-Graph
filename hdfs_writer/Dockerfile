FROM python:2.7-slim
WORKDIR /hdfs_writer
COPY . .

RUN pip install hdfs
RUN pip install kafka-python

# CMD ["python", "tmp.py"]
CMD ["python", "hdfs_write.py"]
