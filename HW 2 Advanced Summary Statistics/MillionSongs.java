import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.io.IOException;
import java.lang.StringBuilder;

public class MillionSongs{
	public static class MillionSongsMap extends Mapper <LongWritable, Text, Text, NullWritable> {
		public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			Text record = new Text( );
			NullWritable nullValue = NullWritable.get( );
			
			String [ ] row = value.toString( ).split( "," );//(?=([^\"]*\"[^\"]*\")*[^\"]*$)" ); 
			StringBuilder strBuilder = new StringBuilder( );
			
			try {
				int year = Integer.parseInt( row[ 165 ] );

				if ( year >= 2000 && year <= 2010 ) {
					strBuilder.append( row[ 2 ] ).append( "\t" )
						  .append( row[ 3 ] ).append( "\t" )
						  .append( row[ 1 ] ).append( "\t" )
						  .append( row[ 165 ] );

					record.set( strBuilder.toString( ) );
					context.write( record, nullValue );
				}
			}
			catch ( NumberFormatException e ) { }
		}
	}
	
	public static void main( String args[ ] ) throws Exception {
		Configuration conf = new Configuration( );
		Job job = Job.getInstance( conf );
		
		job.setJobName( "Million Songs 1" );
		job.setJarByClass( MillionSongs.class );
		
		job.setNumReduceTasks( 0 );
		job.setMapperClass( MillionSongsMap.class );
		
		job.setMapOutputKeyClass( Text.class );
		job.setMapOutputValueClass( NullWritable.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( NullWritable.class );

		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		
		Path in = new Path( args[ 0 ] );
		Path out = new Path( args[ 1 ] );
		
		FileInputFormat.setInputPaths( job, in );
		FileOutputFormat.setOutputPath( job, out );
		
		job.waitForCompletion( true );
	}
}

