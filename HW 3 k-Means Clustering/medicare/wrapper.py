import os;
import sys;
import numpy as np;

if __name__ == "__main__":
    hdfs_prefix = "medicare/";
    data = "/home/public/course/clustering/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt";

    data_hdfs = hdfs_prefix + sys.argv[ 1 ];
    k = sys.argv[ 2 ];
    max_iter = sys.argv[ 3 ];
    meanstddev_hdfs = hdfs_prefix + sys.argv[ 4 ];
    output_hdfs = hdfs_prefix + sys.argv[ 5 ];
    
    print( "\nComputing Mean and Std Dev\n" );
    os.system( "hadoop fs -rm -r " + meanstddev_hdfs );
    os.system( "hadoop jar MeanStdDev.jar " + data_hdfs + " " + meanstddev_hdfs );
    os.system( "hadoop fs -getmerge " + meanstddev_hdfs + " temp" );
    os.system( "hadoop fs -rm -r " + meanstddev_hdfs );
    os.system( "hadoop fs -put temp " + meanstddev_hdfs );

    centroids = "centroids";
    centroids_hdfs = hdfs_prefix + centroids;

    varlist = np.array( [ 21, 24, 26 ] );
    varsfile = "vars";
    varsfile_hdfs = hdfs_prefix + varsfile;
    np.savetxt( varsfile, varlist - 1, "%d", " ", " " );

    os.system( "hadoop fs -rm " + varsfile_hdfs );
    os.system( "hadoop fs -put " + varsfile + " " + varsfile_hdfs );

    varstring = np.array_str( varlist ).strip( "[]" ).split( " " );
    varstring = ",".join( varstring );

    print( "\nRunning k-Means\n" );
    os.system( "shuf -n " + str( k ) + " " + data + " | cut -f" + varstring + " > " + centroids );
    os.system( "hadoop fs -rm " + centroids_hdfs );
    os.system( "hadoop fs -put " + centroids + " " + centroids_hdfs );
    os.system( "hadoop fs -rm -r " + output_hdfs );
    os.system( "hadoop jar kMeans.jar " + data_hdfs + " " + centroids_hdfs  + " " +
                varsfile_hdfs + " " +  meanstddev_hdfs + " " + max_iter + " " + output_hdfs );
    os.system( "hadoop fs -getmerge " + output_hdfs + " " + centroids );
    os.system( "rm ." + centroids + ".crc" );

    print( "\nAssigning Clusters\n" );
    os.system( "hadoop fs -rm -r " + output_hdfs );
    os.system( "hadoop jar ClusterAssignment.jar " + data_hdfs + " " + centroids_hdfs  + " " +
                varsfile_hdfs + " " +  meanstddev_hdfs + " " + output_hdfs );
    os.system( "hadoop fs -getmerge " + output_hdfs + " " + sys.argv[ 1 ] + "_new" );
    
    print( "\nMapReduce Result\n" );
    os.system( "cat " + centroids );
