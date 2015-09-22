import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DataSubset {
	public static class kMeansMap extends Mapper <LongWritable, Text, NullWritable, Text> {
		@Override
		public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			NullWritable nullKey = NullWritable.get( );
			String [ ] line = value.toString( ).split( "\\t" );
			
			int [ ] numericFields = { 18, 23 };
			double [ ] maxFieldVal = { 500, 1000 };
			double fieldVal;
			boolean emitFlag = true;
			
			for ( int i = 0; i < numericFields.length; i++ ) {
				fieldVal = Double.parseDouble( line[ numericFields[ i ] ] );
				
				if ( fieldVal > maxFieldVal[ i ] )
					emitFlag = false;
			}
			
			if ( !line[ 6 ].equals( "I" ) )
				emitFlag = false;
			
			if ( !line[ 13 ].equals( "Internal Medicine") && !line[ 13 ].equals( "Family Practice" ) )
				emitFlag = false;
			
			if ( emitFlag )
				context.write( nullKey, value );
		}
	}
	
	public static void main( String [ ] args ) throws Exception {
		Path inPath, outPath;
		
		inPath = new Path( args[ 0 ] );
		outPath = new Path( args[ 1 ] );

		Configuration conf = new Configuration( );
		Job job = Job.getInstance( conf );
		
		job.setJobName( "Data Subset" );
		job.setJarByClass( DataSubset.class );
		
		job.setMapperClass( kMeansMap.class );
		job.setNumReduceTasks( 0 );
		
		job.setMapOutputKeyClass( NullWritable.class );
		job.setMapOutputValueClass( Text.class );
		job.setOutputKeyClass( NullWritable.class );
		job.setOutputValueClass( Text.class );
		
		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		
		FileInputFormat.setInputPaths( job, inPath );
		FileOutputFormat.setOutputPath( job, outPath );
		
		job.waitForCompletion( true );
	}
}

