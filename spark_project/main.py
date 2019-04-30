#!/usr/local/bin/python
import subprocess


# Start BATCH and REAL-TIME process
rt_proc = subprocess.Popen(["/spark/bin/spark-submit", 
                    "--jars", "spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar", "real_time.py"])

b_proc = subprocess.Popen(["/spark/bin/spark-submit", "batch.py", str(rt_proc.pid) ])


b_proc.wait()
rt_proc.wait()
