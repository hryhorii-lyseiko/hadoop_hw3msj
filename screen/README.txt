RUN
put folder and file with data into hdfs:
hadoop fs -put /root/hw3/data/ inputhw3data
hadoop fs -put /root/hw3/city/city inputhw3city
run jar:
hadoop jar /root/hw3/hw3msj.jar AppDriver inputhw3city inputhw3data outputdatahw3 
READ RESULT
check the existence of output data:
hadoop fs -ls outputdatahw3/
read output from each reducer
hadoop fs -cat outputdatahw3/part-r-00000
hadoop fs -cat outputdatahw3/part-r-00001
hadoop fs -cat outputdatahw3/part-r-00002
hadoop fs -cat outputdatahw3/part-r-00003
hadoop fs -cat outputdatahw3/part-r-00004
hadoop fs -cat outputdatahw3/part-r-00005
