import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
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

import java.lang.StringBuilder;

public class NGram {
	public static class Pair implements Writable {
		private int first;
		private int second;
		
		public Pair( ) {
			first = 0;
			second = 0;
		}
		
		public Pair( int first, int second ) {
			this.first = first;
			this.second = second;
		}
		
		@Override
		public void readFields( DataInput in ) throws IOException {
			first = in.readInt( );
			second = in.readInt( );
		}
		
		@Override
		public void write( DataOutput out ) throws IOException {
			out.writeInt( first );
			out.writeInt( second );
		}

		public int getFirst( ) {
			return first;
		}

		public int getSecond( ) {
			return second;
		}

		public void setFirst( int first ) {
			this.first = first;
		}

		public void setSecond( int second ) {
			this.second = second;
		}
	}
	
	public static class NGram1Map extends Mapper <LongWritable, Text, Text, Pair> {
		public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			Text combination = new Text( );
			Pair volumesPair = new Pair( );
			
			String[ ] row = value.toString( ).split( "\\s+" );
			StringBuilder strBuilder;
			
			int year, numVol;
			
			String[ ] substr = { "nu", "die", "kla" };
			
			for ( String item: substr ) {
				if ( row[ 0 ].toLowerCase( ).contains( item ) ) {
					try {
						year = Integer.parseInt( row[ 1 ] );
						numVol = Integer.parseInt( row[ 3 ] );
						
						strBuilder = new StringBuilder( row[ 1 ] );
						strBuilder.append( " | " ).append( item );
						
						combination.set( strBuilder.toString( ) );
						volumesPair = new Pair( numVol, 1 );
						
						context.write( combination, volumesPair );
					}
					catch ( NumberFormatException e ) { }
				}
			}
		}
	}
	
	public static class NGram2Map extends Mapper <LongWritable, Text, Text, Pair> {
		public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			Text combination = new Text( );
			Pair volumesPair;
			
			String[ ] row = value.toString( ).split( "\\s+" );
			StringBuilder strBuilder;
			
			int year, numVol;
			
			String[ ] substr = { "nu", "die", "kla" };
			
			for ( String item: substr ) {
				if ( row[ 0 ].toLowerCase( ).contains( item ) || row[ 1 ].toLowerCase( ).contains( item ) ) {
					try {
						year = Integer.parseInt( row[ 2 ] );
						numVol = Integer.parseInt( row[ 4 ] );
						
						strBuilder = new StringBuilder( row[ 2 ] );
						strBuilder.append( " | " ).append( item );
						
						combination.set( strBuilder.toString( ) );
						volumesPair = new Pair( numVol, 1 );
						
						context.write( combination, volumesPair );
					}
					catch ( NumberFormatException e ) { }
				}
			}
		}
	}
	
	public static class NGramCombine extends Reducer <Text, Pair, Text, Pair> {
		public void reduce( Text key, Iterable <Pair> values, Context context ) throws IOException, InterruptedException {
			Pair volumesPair = new Pair( );
			int count = 0;
			int volumes = 0;

			for ( Pair value: values ) {
				volumes += value.getFirst( );
				count += value.getSecond( );
			}

			volumesPair.setFirst( volumes );
			volumesPair.setSecond( count );

			context.write( key, volumesPair );
		}
	}
	
	public static class NGramReduce extends Reducer <Text, Pair, Text, DoubleWritable> {
		public void reduce( Text key, Iterable <Pair> values, Context context ) throws IOException, InterruptedException {
			DoubleWritable averageVolumes = new DoubleWritable( );
			int count = 0;
			double volumes = 0;
			
			for ( Pair value: values ) {
				volumes += value.getFirst( );
				count += value.getSecond( );
			}
			
			averageVolumes.set( volumes / count );
			context.write( key, averageVolumes );
		}
	}
	
	public static void main( String args[ ] ) throws Exception {
		Configuration conf = new Configuration( );
		Job job = Job.getInstance( conf );
		
		job.setJobName( "Google nGrams" );
		job.setJarByClass( NGram.class );
		
		job.setCombinerClass( NGramCombine.class );
		job.setReducerClass( NGramReduce.class );
		
		job.setMapOutputKeyClass( Text.class );
		job.setMapOutputValueClass( Pair.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( DoubleWritable.class );

		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		
		Path in1 = new Path( args[ 0 ] );
		Path in2 = new Path( args[ 1 ] );
		Path out = new Path( args[ 2 ] );
		
		MultipleInputs.addInputPath( job, in1, TextInputFormat.class, NGram1Map.class );
		MultipleInputs.addInputPath( job, in2, TextInputFormat.class, NGram2Map.class );
		FileOutputFormat.setOutputPath( job, out );
		
		job.waitForCompletion( true );
	}
}

