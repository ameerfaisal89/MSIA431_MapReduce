This project implements k-means clustering for large distributed datasets using Hadoop MapReduce. There are several jobs, each contained in the following directories:

datasubset - Contains map-only MapReduce job for subsetting the original Medicare dataset
meanstddev - Contains MapReduce job for computing mean and standard deviation for various fields to normalize data
clustering - Contains MapReduce jobs for normalizing data and performing k-means clustering and returns the final 	     centroids
assignment - Contains a map-only MapReduce job for assigning cluster labels to data points based on the final centroids

medicare - Contains an end-to-end clustering framework, that takes as input the data, the number of clusters (k), 
	   the maximum number of iterations for k-means, and output paths. The program is run with a wrapper script in
	   python that first calls the meansstddev MapReduce job, and then the k-means job for the selected variables.