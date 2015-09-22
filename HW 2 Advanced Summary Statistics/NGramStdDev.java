import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NGramStdDev {
	public static class Triplet implements Writable {
		private long first;
		private long second;
		private long third;
		
		public Triplet( ) {
			first = 0;
			second = 0;
			third = 0;
		}
		
		public Triplet( long first, long second, long third ) {
			this.first = first;
			this.second = second;
			this.third = third;
		}
		
		@Override
		public void readFields( DataInput in ) throws IOException {
			first = in.readLong( );
			second = in.readLong( );
			third = in.readLong( );
		}
		
		@Override
		public void write( DataOutput out ) throws IOException {
			out.writeLong( first );
			out.writeLong( second );
			out.writeLong( third );
		}

		public long getFirst( ) {
			return first;
		}

		public long getSecond( ) {
			return second;
		}
		
		public long getThird( ) {
			return third;
		}

		public void setFirst( long first ) {
			this.first = first;
		}

		public void setSecond( long second ) {
			this.second = second;
		}
		
		public void setThird( long third ) {
			this.third = third;
		}
	}
	
	public static class StdDev1Map extends Mapper <LongWritable, Text, NullWritable, Triplet> {
		public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			Triplet volumesTriplet = new Triplet( );
			NullWritable nullKey = NullWritable.get( );
			
			String[ ] row = value.toString( ).split( "\\s+" );
			
			long numVol;
			
			try {
				numVol = Long.parseLong( row[ 3 ] );
				volumesTriplet.setFirst( 1 );
				volumesTriplet.setSecond( numVol );
				volumesTriplet.setThird( numVol * numVol );
				
				context.write( nullKey, volumesTriplet );
			}
			catch ( NumberFormatException e ) { }
		}
	}
	
	public static class StdDev2Map extends Mapper <LongWritable, Text, NullWritable, Triplet> {
		public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			Triplet volumesTriplet = new Triplet( );
			NullWritable nullKey = NullWritable.get( );
			
			String[ ] row = value.toString( ).split( "\\s+" );
			
			long numVol;
			
			try {
				numVol = Long.parseLong( row[ 4 ] );
				volumesTriplet.setFirst( 1 );
				volumesTriplet.setSecond( numVol );
				volumesTriplet.setThird( numVol * numVol );
				
				context.write( nullKey, volumesTriplet );
			}
			catch ( NumberFormatException e ) { }
		}
	}
	
	public static class StdDevCombine extends Reducer <NullWritable, Triplet, NullWritable, Triplet> {
		public void reduce( NullWritable key, Iterable <Triplet> values, Context context ) throws IOException, InterruptedException {
			Triplet volumesTriplet = new Triplet( );
			long count = 0;
			long volumes = 0;
			long volumesSq = 0;

			for ( Triplet value: values ) {
				count += value.getFirst( );
				volumes += value.getSecond( );
				volumesSq += value.getThird( );
			}
			
			volumesTriplet.setFirst( count );
			volumesTriplet.setSecond( volumes );
			volumesTriplet.setThird( volumesSq );

			context.write( key, volumesTriplet );
		}
	}
		
	public static class StdDevReduce extends Reducer <NullWritable, Triplet, NullWritable, DoubleWritable> {
		public void reduce( NullWritable key, Iterable <Triplet> values, Context context ) throws IOException, InterruptedException {
			long count = 0;
			double volumes = 0;
			double volumesSq = 0;
			
			double mean, stdDev;

			for ( Triplet value: values ) {
				count += value.getFirst( );
				volumes += value.getSecond( );
				volumesSq += value.getThird( );
			}

			mean = volumes / count;
			stdDev = volumesSq - count * Math.pow( mean, 2 );
			stdDev = Math.sqrt( stdDev / count );

			context.write( key, new DoubleWritable( stdDev ) );
		}
	}
	
	public static void main( String args[ ] ) throws Exception {
		Configuration conf = new Configuration( );
		Job job = Job.getInstance( conf );
		
		job.setJobName( "Google Std Deviation" );
		job.setJarByClass( NGramStdDev.class );
		
		job.setCombinerClass( StdDevCombine.class );
		job.setReducerClass( StdDevReduce.class );
		job.setNumReduceTasks( 1 );
		
		job.setMapOutputKeyClass( NullWritable.class );
		job.setMapOutputValueClass( Triplet.class );
		job.setOutputKeyClass( NullWritable.class );
		job.setOutputValueClass( DoubleWritable.class );

		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		
		Path in1 = new Path( args[ 0 ] );
		Path in2 = new Path( args[ 1 ] );
		Path out = new Path( args[ 2 ] );
		
		MultipleInputs.addInputPath( job, in1, TextInputFormat.class, StdDev1Map.class );
		MultipleInputs.addInputPath( job, in2, TextInputFormat.class, StdDev2Map.class );
		FileOutputFormat.setOutputPath( job, out );
		
		job.waitForCompletion( true );
	}
}

