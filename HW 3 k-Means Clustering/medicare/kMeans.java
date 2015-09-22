import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.lang.StringBuilder;

public class kMeans {
	public static class PartialMeansWritable implements Writable {
		private int count;
		private ArrayList <Double> meansList;
		
		public PartialMeansWritable( ) {
			this.count = 1;
			this.meansList = null;
		}
		
		public PartialMeansWritable( int count, ArrayList <Double> meansList ) {
			this.count = count;
			this.meansList = meansList;
		}
		
		@Override
		public void readFields( DataInput in ) throws IOException {
			int size;
			
			this.count = in.readInt( );
			size = in.readInt( );
			this.meansList = new ArrayList <Double>( size );
			
			for ( int i = 0; i < size; i++ )
				meansList.add( in.readDouble( ) );
		}
		
		@Override
		public void write( DataOutput out ) throws IOException {
			int size = 0;
			
			if ( this.meansList != null ) {
				size = this.meansList.size( );
			}
			
			out.writeInt( this.count );
			out.writeInt( size );
			
			for ( int i = 0; i < size; i++ )
				out.writeDouble( this.meansList.get( i ) );
		}
		
		public void setCount( int count ) {
			this.count = count;
		}
		
		public void setMeansList( ArrayList <Double> meansList ) {
			this.meansList = meansList;
		}
		
		public int getCount( ) {
			return this.count;
		}
		
		public ArrayList <Double> getMeansList( ) {
			return this.meansList;
		}
	}
	
	public static class kMeansMap extends Mapper <LongWritable, Text, IntWritable, PartialMeansWritable> {
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
			boolean isFirstIter = conf.get( "isFirstIter" ).equals( "true" );
			int index;
			double tempVal;
			
			if ( cacheFiles != null && cacheFiles.length > 0 )
				for ( int i = 0; i < cacheFiles.length; i++ )
					readCacheFile( cacheFiles[ i ], options[ i ] );

			if ( isFirstIter ) {
				for ( int i = 0; i < centroidsList.size( ); i++ ) {
					centroid = new ArrayList <Double>( centroidsList.get( i ) );

					for ( int j = 0; j < varList.size( ); j++ ) {
						index = varList.get( j );
						tempVal = ( centroidsList.get( i ).get( j ) - meansList.get( index ) ) / stdDevList.get( index );
						centroid.set( j, tempVal );
					}

					centroidsList.set( i, centroid );
				}
			}
		}
		
		@Override
		public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			String [ ] line = value.toString( ).split( "\\t" );
			ArrayList <Double> row = new ArrayList <Double>( );
			
			PartialMeansWritable rowValue = new PartialMeansWritable( );
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
			rowValue.setMeansList( row );
			rowValue.setCount( 1 );
			
			context.write( clusterKey, rowValue );
		}
	}

	public static class kMeansCombine extends Reducer <IntWritable, PartialMeansWritable, IntWritable, PartialMeansWritable> {
		@Override
		public void reduce( IntWritable key, Iterable <PartialMeansWritable> values, Context context ) throws IOException, InterruptedException {
			PartialMeansWritable partialMeansValue = new PartialMeansWritable( );

			ArrayList <Double> partialMeans = null;
			ArrayList <Double> row;
			int partialCount = 0, rowCount;

			double tempVal;

			for ( PartialMeansWritable val: values ) {
				row = val.getMeansList( );
				rowCount = val.getCount( );
				
				if ( partialMeans == null ) {
					partialMeans = new ArrayList <Double>( );

					for ( int i = 0; i < row.size( ); i++ )
						partialMeans.add( rowCount * row.get( i ) );
				}
				else
					for ( int i = 0; i < row.size( ); i++ ) {
						tempVal = partialMeans.get( i ) + rowCount * row.get( i );
						partialMeans.set( i, tempVal );
					}

				partialCount += rowCount;
			}

			for ( int i = 0; i < partialMeans.size( ); i++ )
				partialMeans.set( i, partialMeans.get( i ) / partialCount );

			partialMeansValue.setMeansList( partialMeans );
			partialMeansValue.setCount( partialCount );
			context.write( key, partialMeansValue );
		}
	}
	
	public static class kMeansReduce extends Reducer <IntWritable, PartialMeansWritable, NullWritable, Text> {
		@Override
		public void reduce( IntWritable key, Iterable <PartialMeansWritable> values, Context context ) throws IOException, InterruptedException {
			NullWritable nullKey = NullWritable.get( );
			Text meansText = new Text( );
			ArrayList <Double> means = null;
			ArrayList <Double> partialMeans;
			int count = 0, partialCount;
			
			StringBuilder sBuilder = new StringBuilder( );
			String prefix = "";
			double tempVal;
			
			for ( PartialMeansWritable val: values ) {
				partialMeans = val.getMeansList( );
				partialCount = val.getCount( );
				
				if ( means == null ) {
					means = new ArrayList <Double>( );

					for ( int i = 0; i < partialMeans.size( ); i++ )
						means.add( partialCount * partialMeans.get( i ) );
				}
				else
					for ( int i = 0; i < partialMeans.size( ); i++ ) {
						tempVal = means.get( i ) + partialCount * partialMeans.get( i );
						means.set( i, tempVal );
					}
				
				count += partialCount;
			}
			
			for ( int i = 0; i < means.size( ); i++ )
				means.set( i, means.get( i ) / count );
			
			for ( int i = 0; i < means.size( ); i++ ) {
				sBuilder.append( prefix ).append( means.get( i ) );

				if ( i == 0 )
					prefix = "\t";
			}
			
			meansText.set( sBuilder.toString( ) );
			context.write( nullKey, meansText );
		}
	}

	private static ArrayList <ArrayList <Double>> readCentroids( FileSystem fs, Path centroidsFile ) throws IOException {
		ArrayList <ArrayList <Double>> centroidsList = new ArrayList <ArrayList <Double>>( );
		ArrayList <Double> centroid;

		InputStreamReader inReader = null;
		BufferedReader bReader = null;
		String [ ] row;
		String line;
		
		try {
			inReader = new InputStreamReader( fs.open( centroidsFile ) );
			bReader = new BufferedReader( inReader );
			
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
		catch ( FileNotFoundException e ) {
			e.printStackTrace( );
		}
		finally {
			bReader.close( );
			inReader.close( );
		}

		return centroidsList;
	}

	public static boolean hasConverged( FileSystem hdfs, Path centroidsOld, Path centroidsNew ) throws IOException {
		ArrayList <ArrayList <Double>> centroidsListOld, centroidsListNew;
		double tol = 1e-2;
		double dist;

		centroidsListOld = readCentroids( hdfs, centroidsOld );
		centroidsListNew = readCentroids( hdfs, centroidsNew );

		for ( int i = 0; i < centroidsListOld.size( ); i++ ) {
			dist = 0;

			for ( int j = 0; j < centroidsListOld.get( i ).size( ); j++ )
				dist += Math.pow( centroidsListOld.get( i ).get( j ) - centroidsListNew.get( i ).get( j ), 2 );

			if ( dist > tol )
				return false;
		}

		return true;
	}
	
	public static void main( String [ ] args ) throws Exception {
		final int maxIter;
		
		Path inPath, centroidsPath, varListPath, meanStdDevPath, outPath, tempCentroidsPath;

		int iter = 0, maxIterInput = 0;
		boolean converged = false;
		
		FileSystem hdfs = null;
		Configuration conf = null;
		Job job = null;
		
		inPath = new Path( args[ 0 ] );
		centroidsPath = new Path( args[ 1 ] );
		varListPath = new Path( args[ 2 ] );
		meanStdDevPath = new Path( args[ 3 ] );
		outPath = new Path( args[ 5 ] );

		tempCentroidsPath = new Path( args[ 1 ] + "_temp" );

		try {
			maxIterInput = Integer.parseInt( args[ 4 ] );
		}
		catch ( NumberFormatException e ) {
			e.printStackTrace( );
		}
		
		maxIter = maxIterInput;

		while ( !converged && iter < maxIter ) {
			conf = new Configuration( );

			if ( iter == 0 )
				conf.set( "isFirstIter", "true" );
			else
				conf.set( "isFirstIter", "false" );

			job = Job.getInstance( conf );
			
			hdfs = FileSystem.get( conf );
			
			job.setJobName( "k-means" );
			job.setJarByClass( kMeans.class );
			
			job.setMapperClass( kMeansMap.class );
			job.setCombinerClass( kMeansCombine.class );
			job.setReducerClass( kMeansReduce.class );
			
			job.setMapOutputKeyClass( IntWritable.class );
			job.setMapOutputValueClass( PartialMeansWritable.class );
			job.setOutputKeyClass( NullWritable.class );
			job.setOutputValueClass( Text.class );
			
			job.setInputFormatClass( TextInputFormat.class );
			job.setOutputFormatClass( TextOutputFormat.class );
			
			DistributedCache.addCacheFile( centroidsPath.toUri( ), job.getConfiguration( ) );
			DistributedCache.addCacheFile( varListPath.toUri( ), job.getConfiguration( ) );
			DistributedCache.addCacheFile( meanStdDevPath.toUri( ), job.getConfiguration( ) );
			
			FileInputFormat.setInputPaths( job, inPath );
			FileOutputFormat.setOutputPath( job, outPath );
			
			job.waitForCompletion( true );
			
			FileUtil.copyMerge( hdfs, outPath, hdfs, tempCentroidsPath, true, conf, "" );

			converged = hasConverged( hdfs, centroidsPath, tempCentroidsPath );

			FileUtil.copy( hdfs, tempCentroidsPath, hdfs, centroidsPath, true, conf );
			iter++;
		}
		
		FileUtil.copy( hdfs, centroidsPath, hdfs, outPath, false, conf );

		if ( !converged )
			System.out.println( "\nk-Means did not converge in " + maxIter + " iterations.\n"  );
		else
			System.out.println( "\nNumber of iterations to converge: " + iter + ".\n" );
	}
}

