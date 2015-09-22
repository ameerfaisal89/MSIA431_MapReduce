import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MusicMax {
	public static class MusicMap extends Mapper <LongWritable, Text, Text, DoubleWritable> {
		public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			Text artist = new Text( );
			DoubleWritable duration = new DoubleWritable( );
			
			String [ ] row = value.toString( ).split( "," );//(?=([^\"]*\"[^\"]*\")*[^\"]*$)" );
			
			artist.set( row[ 2 ] );
			
			try {
				duration.set( Double.parseDouble( row[ 3 ] ) );
				context.write( artist, duration );
			}
			catch ( NumberFormatException e ) { }
		}
	}

	public static class MusicPartition extends Partitioner <Text, DoubleWritable> {
		public int getPartition( Text key, DoubleWritable value, int numPartitions ) {
			char keyChar = key.toString( ).charAt( 0 );
			int partNum = 0;

			char [ ] breakPoints = { 'F', 'K', 'P', 'U' };

			if ( numPartitions == 0 )
				return 0;

			for ( char splitChar: breakPoints ) {
				if (  keyChar < splitChar )
					break;

				partNum++;
			}

			return partNum % numPartitions;
			/*
			if ( numPartitions > 0 ) {
				partNum = ( Character.toLowerCase( keyChar ) - 'a' ) / numPartitions;
				
				if ( partNum < 0 )
					partNum = 0;
				if ( partNum >= numPartitions )
					partNum = numPartitions - 1;
				return partNum;
			}
			
			else
				return 0;*/
		}
	}

	public static class MusicReduce extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce ( Text key, Iterable <DoubleWritable> values, Context context ) throws IOException, InterruptedException {
			DoubleWritable maxDuration = new DoubleWritable( );
			double maxDur = 0;
			double dur;

			for ( DoubleWritable duration: values ) {
				dur = duration.get( );

				if ( maxDur < dur )
					maxDur = dur;
			}

			maxDuration.set( maxDur );
			context.write( key, maxDuration );
		}
	}
	
	public static void main( String args[ ] ) throws Exception {
		Configuration conf = new Configuration( );
		Job job = Job.getInstance( conf );
		
		job.setJobName( "Million Songs 2" );
		job.setJarByClass( MusicMax.class );
		
		job.setMapperClass( MusicMap.class );
		job.setCombinerClass( MusicReduce.class );
		job.setPartitionerClass( MusicPartition.class );
		job.setReducerClass( MusicReduce.class );
		job.setNumReduceTasks( 5 );
		
		job.setMapOutputKeyClass( Text.class );
		job.setMapOutputValueClass( DoubleWritable.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( DoubleWritable.class );

		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		
		Path in = new Path( args[ 0 ] );
		Path out = new Path( args[ 1 ] );
		
		FileInputFormat.setInputPaths( job, in );
		FileOutputFormat.setOutputPath( job, out );
		
		job.waitForCompletion( true );
	}
}
