wrapper.py takes the following inputs in order: dataset on hdfs, number of clusters (k), maximum number of iterations, filename to store mean and std dev on HDFS and output folder on HDFS.

Final centroids will be stored in file "centroids" on the local filesystem.

Sample command:
python wrapper.py data 5 20 meanstddev out
