import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Temperature2 {
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
	private static final List<Integer> quality = Arrays.asList( 0, 1, 4, 5, 9 );
	private IntWritable recTemp = new IntWritable( );
	private Text year = new Text( );

	public void map( Object key, Text value, Context context ) throws IOException, InterruptedException {
	    String line = value.toString( );
	    int temp = Integer.parseInt( line.substring( 88, 92 ) );				// Assign the temperature value
	    int qualityVal = Integer.parseInt( line.substring( 92, 93 ) );			// Assign the quality indicator

	    if ( line.charAt( 87 ) == '-' )							// Check for sign of the temperature
		temp = -temp;

	    if ( temp != 9999 && quality.contains( qualityVal ) ) {				// If temp not missing and quality is acceptable, emit
		recTemp.set( temp );								// Set value as temperature
		year.set( line.substring( 15, 19 ) );						// Set key as year
		context.write( year, recTemp );
	    }
	}
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce( Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
	    int temp, maxTemp = -9999;

	    for ( IntWritable val: values ) {
		temp = val.get( );								// Get all temperatures for a year 

		if ( maxTemp < temp )								// Set max temperature as current temperature
		    maxTemp = temp;								// value if max value is smaller
	    }

	    context.write( key, new IntWritable( maxTemp ) );
	}
    }

    public static void main( String[ ] args ) throws Exception {
	Configuration conf = new Configuration( );
	Job job = Job.getInstance( conf, "Temperature 2" );

	job.setJarByClass( Temperature2.class );
	job.setMapperClass( Map.class );
	job.setCombinerClass( Reduce.class );
	job.setReducerClass( Reduce.class );
	job.setOutputKeyClass( Text.class );
	job.setOutputValueClass( IntWritable.class );
	FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
	FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );
	System.exit(job.waitForCompletion( true ) ? 0 : 1 );
    }
}

