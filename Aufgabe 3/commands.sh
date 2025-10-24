# a)

hdfs dfs -help
hdfs dfs -usage mkdir

# b)
hdfs dfs -mkdir /home/heiserervalentin/hdfstest

# c)
echo "Hello World" > test.txt
hdfs dfs -put test.txt /home/heiserervalentin/hdfstest

# d)
hdfs dfs -cat /home/heiserervalentin/hdfstest/test.txt

# e)
hdfs dfs -get /home/heiserervalentin/hdfstest/test.txt ./test_from_hdfs.txt

# f)
hdfs dfs -ls /home/heiserervalentin/hdfstest

# g)
hdfs dfs -mkdir /home/heiserervalentin/hdfstest2
hdfs dfs -mv /home/heiserervalentin/hdfstest/test.txt /home/heiserervalentin/hdfstest2/
hdfs dfs -cp /home/heiserervalentin/hdfstest2/test.txt /home/heiserervalentin/hdfstest2/test_copy.txt

# h)
hdfs dfs -head /home/heiserervalentin/hdfstest2/test.txt
hdfs dfs -tail /home/heiserervalentin/hdfstest2/test.txt

# i)
hdfs dfs -df -h
hdfs dfs -du /home/heiserervalentin

# j)
hdfs dfs -setrep 2 /home/heiserervalentin

# k)
hdfs dfs -rm /home/heiserervalentin/hdfstest2/test_copy.txt
hdfs dfs -rm /home/heiserervalentin/hdfstest2/test.txt
hdfs dfs -rmdir /home/heiserervalentin/hdfstest2
hdfs dfs -rmdir /home/heiserervalentin/hdfstest

hdfs dfs -ls /home/heiserervalentin
