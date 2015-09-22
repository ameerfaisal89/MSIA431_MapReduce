import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

import java.io.File;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.ArrayList;

public class ClusterAssignment {
	public static class kMeansMap extends Mapper <LongWritable, Text, IntWritable, Text> {
		private static ArrayList <ArrayList <Double>> centroidsList = new ArrayList <ArrayList <Double>>( );
		private static ArrayList <Integer> varList = new ArrayList <Integer>( );
		private static ArrayList <Double> meansList = new ArrayList <Double>( );
		private static ArrayList <Double> stdDevList = new ArrayList <Double>( );
		
		private double computeDist( ArrayList <Double> x, ArrayList <Double> y ) throws Exception {
			double dist = 0;
			
			if ( x.size( ) != y.size( ) )
				throw new Exception( "Lengths of records do not match." );
			
			for ( int i = 0; i < x.size( ); i++ )
				dist += Math.pow( x.get( i ) - y.get( i ), 2 );
			
			return Math.sqrt( dist );
		}

		private void loadCentroids( BufferedReader bReader ) throws IOException {
			ArrayList <Double> centroid;
			String [ ] row;
			String line;

			line = bReader.readLine( );
			
			while ( line != null ) {
				centroid = new ArrayList <Double>( );
				row = line.split( "\\s+" );
					
				for ( String field: row )
					centroid.add( Double.parseDouble( field ) );
				
				centroidsList.add( centroid );
				line = bReader.readLine( );
			}
		}

		private void getVarList( BufferedReader bReader ) throws IOException {
			String [ ] varListArray;
			String line;
			
			line = bReader.readLine( );
			
			if ( line != null ) {
				varListArray = line.split( "\\s+" );
					
				for ( String var: varListArray )
					varList.add( Integer.parseInt( var ) );
			}
		}
		
		private void getMeanStdDev( BufferedReader bReader ) throws IOException {
			String [ ] paramsArray;
			String line;
			
			line = bReader.readLine( );
			
			if ( line != null ) {
				paramsArray = line.split( "\\s+" );
					
				for ( String param: paramsArray )
					meansList.add( Double.parseDouble( param ) );
			}
			
			line = bReader.readLine( );

			if ( line != null ) {
				paramsArray = line.split( "\\s+" );
					
				for ( String param: paramsArray )
					stdDevList.add( Double.parseDouble( param ) );
			}
		}
		
		private void readCacheFile( Path cacheFile, String option ) throws IOException {
			FileReader fReader = null;
			BufferedReader bReader = null;
			
			try {
				fReader = new FileReader( new File( cacheFile.toString( ) ) );
				bReader = new BufferedReader( fReader );
				
				if ( option.equals( "centroids" ) )
					loadCentroids( bReader );
				else if ( option.equals( "vars" ) )
					getVarList( bReader );
				else if ( option.equals( "meanStdDev" ) )
					getMeanStdDev( bReader );
			}
			catch ( FileNotFoundException e ) {
				e.printStackTrace( );
			}
			finally {
				bReader.close( );
				fReader.close( );
			}
		}
		
		@Override
		public void setup( Context context ) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration( );

			Path [ ] cacheFiles = DistributedCache.getLocalCacheFiles( context.getConfiguration( ) );
			String[ ] options = { "centroids", "vars", "meanStdDev" };

			ArrayList <Double> centroid = null;
			int index;
			double tempVal;
			
			if ( cacheFiles != null && cacheFiles.length > 0 )
				for ( int i = 0; i < cacheFiles.length; i++ )
					readCacheFile( cacheFiles[ i ], options[ i ] );
		}
		
		@Override
		public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			String [ ] line = value.toString( ).split( "\\t" );
			ArrayList <Double> row = new ArrayList <Double>( );
			IntWritable clusterKey = new IntWritable( );
			
			int index, clusterNum = -1;
			double dist, minDist = -1;
			double tempVal;
			
			for ( int i = 0; i < varList.size( ); i++ ) {
				index = varList.get( i );

				if ( index < line.length ) {
					tempVal = ( Double.parseDouble( line[ index ] ) - meansList.get( index ) ) / stdDevList.get( index );
					row.add( tempVal );
				}
			}
			
			try {
				for ( int i = 0; i < centroidsList.size( ); i++ ) {
					dist = computeDist( row, centroidsList.get( i ) );
					
					if ( minDist < 0 || dist < minDist ) {
						minDist = dist;
						clusterNum = i;
					}
				}
			}
			catch ( Exception e ) {
				e.printStackTrace( );
			}
			
			clusterKey.set( clusterNum );
			context.write( clusterKey, value );
		}
	}
	
	public static void main( String [ ] args ) throws Exception {
		Path inPath, centroidsPath, varListPath, meanStdDevPath, outPath;
		
		inPath = new Path( args[ 0 ] );
		centroidsPath = new Path( args[ 1 ] );
		varListPath = new Path( args[ 2 ] );
		meanStdDevPath = new Path( args[ 3 ] );
		outPath = new Path( args[ 4 ] );

		Configuration conf = new Configuration( );
		Job job = Job.getInstance( conf );
		
		job.setJobName( "Cluster Assignment" );
		job.setJarByClass( ClusterAssignment.class );
		
		job.setMapperClass( kMeansMap.class );
		job.setNumReduceTasks( 0 );
		
		job.setMapOutputKeyClass( IntWritable.class );
		job.setMapOutputValueClass( Text.class );
		job.setOutputKeyClass( IntWritable.class );
		job.setOutputValueClass( Text.class );
		
		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		
		DistributedCache.addCacheFile( centroidsPath.toUri( ), job.getConfiguration( ) );
		DistributedCache.addCacheFile( varListPath.toUri( ), job.getConfiguration( ) );
		DistributedCache.addCacheFile( meanStdDevPath.toUri( ), job.getConfiguration( ) );
		
		FileInputFormat.setInputPaths( job, inPath );
		FileOutputFormat.setOutputPath( job, outPath );
		
		job.waitForCompletion( true );
	}
}

