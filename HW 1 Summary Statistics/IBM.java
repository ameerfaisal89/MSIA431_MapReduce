import java.io.IOException;
import java.util.Iterator;
import java.lang.StringBuilder;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class IBM extends Configured implements Tool {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	private DoubleWritable column4 = new DoubleWritable( );
	private Text combination = new Text( );

	public void configure( JobConf job ) {
	}

	public void map( LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter ) throws IOException {
	    StringBuilder strBuilder = new StringBuilder( );				// StringBuilder object to concatenate strings
	    String[ ] row = value.toString( ).split( "," );				// Split input row delimited by "," to an array of Strings
	    String flag, prefix;
	    
	    prefix = "";

	    for ( int i = 29; i < 33; i++ ) {
		strBuilder.append( prefix ).append( row[ i ] );				// Concatenate columns 30-33 from the input row
		prefix = ", ";
	    }

	    flag = row[ row.length - 1 ];						// Fetch the last column of the row

	    if ( flag.toLowerCase( ).equals( "false" ) ) {				// If last column is false, emit
		combination.set( strBuilder.toString( ) );				// Set key as combination of columns 30-33
		column4.set( Double.parseDouble( row[ 3 ] ) );				// Set value as column 4 value from row
		output.collect( combination, column4 );
	    }	
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce( Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter ) throws IOException {
	    double sum = 0.0;
	    int count = 0;

	    while( values.hasNext( ) ) {						// Do for all values of key
		sum += values.next( ).get( );						// Compute sum and count of values
		count++;
	    }

	    output.collect( key, new DoubleWritable( sum / count ) );			// Emit key and average of column 4 as values
	}
    }

    public int run( String[ ] args ) throws Exception {
	JobConf conf = new JobConf( getConf( ), IBM.class );
	conf.setJobName( "IBM" );

	conf.setOutputKeyClass( Text.class );
	conf.setOutputValueClass( DoubleWritable.class );

	conf.setMapperClass( Map.class );
	conf.setCombinerClass( Reduce.class );
	conf.setReducerClass( Reduce.class );

	conf.setInputFormat( TextInputFormat.class );
	conf.setOutputFormat( TextOutputFormat.class );

	FileInputFormat.setInputPaths( conf, new Path( args[ 0 ] ) );
	FileOutputFormat.setOutputPath(conf, new Path( args[ 1 ] ) );

	JobClient.runJob( conf );
	return 0;
    }

    public static void main( String[ ] args ) throws Exception {
	int res = ToolRunner.run( new Configuration( ), new IBM( ), args );
	System.exit( res );
    }
}

