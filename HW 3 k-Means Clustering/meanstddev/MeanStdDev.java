import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.lang.StringBuilder;

public class MeanStdDev {
	public static class PartialSumsWritable implements Writable {
		private int count;
		private ArrayList <Double> sumList;
		private ArrayList <Double> sumSqList;
		
		public PartialSumsWritable( ) {
			this.count = 0;
			this.sumList = null;
			this.sumSqList = null;
		}
		
		public PartialSumsWritable( int count, ArrayList <Double> sumList, ArrayList <Double> sumSqList ) {
			this.count = count;
			this.sumList = sumList;
			this.sumSqList = sumSqList;
		}
		
		@Override
		public void readFields( DataInput in ) throws IOException {
			int size;
			
			this.count = in.readInt( );
			size = in.readInt( );
			this.sumList = new ArrayList <Double>( size );
			this.sumSqList = new ArrayList <Double>( size );
			
			for ( int i = 0; i < size; i++ )
				sumList.add( in.readDouble( ) );
			for ( int i = 0; i < size; i++ )
				sumSqList.add( in.readDouble( ) );
		}
		
		@Override
		public void write( DataOutput out ) throws IOException {
			int size = 0;
			
			if ( this.sumList != null ) {
				size = this.sumList.size( );
			}
			
			out.writeInt( this.count );
			out.writeInt( size );
			
			for ( int i = 0; i < size; i++ )
				out.writeDouble( this.sumList.get( i ) );
			for ( int i = 0; i < size; i++ )
				out.writeDouble( this.sumSqList.get( i ) );
		}
		
		public void setCount( int count ) {
			this.count = count;
		}
		
		public void setSum( ArrayList <Double> sumList ) {
			this.sumList = sumList;
		}
		
		public void setSumSq( ArrayList <Double> sumSqList ) {
			this.sumSqList = sumSqList;
		}
		
		public int getCount( ) {
			return this.count;
		}
		
		public ArrayList <Double> getSum( ) {
			return this.sumList;
		}
		
		public ArrayList <Double> getSumSq( ) {
			return this.sumSqList;
		}
	}
	
	public static class MeanStdDevMap extends Mapper <LongWritable, Text, NullWritable, PartialSumsWritable> {
		public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			PartialSumsWritable rowValue = new PartialSumsWritable( );
			NullWritable nullKey = NullWritable.get( );
			
			ArrayList <Double> sumList = new ArrayList <Double>( );
			ArrayList <Double> sumSqList = new ArrayList <Double>( );
			
			String[ ] row = value.toString( ).split( "\\t" );
			double paramValue;
			
			for ( int i = 0; i < row.length; i++ ) {
				try {
					paramValue = Double.parseDouble( row[ i ] );
				}
				catch ( NumberFormatException e ) {
					paramValue = Double.NaN;
				}
				
				sumList.add( paramValue );
				sumSqList.add( paramValue * paramValue );
			}
			
			rowValue.setCount( 1 );
			rowValue.setSum( sumList );
			rowValue.setSumSq( sumSqList );
			context.write( nullKey, rowValue );
		}
	}
	
	public static class MeanStdDevCombine extends Reducer <NullWritable, PartialSumsWritable, NullWritable, PartialSumsWritable> {
		public void reduce( NullWritable key, Iterable <PartialSumsWritable> values, Context context ) throws IOException, InterruptedException {
			PartialSumsWritable partialSumsValue = new PartialSumsWritable( );
			ArrayList <Double> partialSumList = null;
			ArrayList <Double> partialSumSqList = null;
			int partialCount = 0;
			
			ArrayList <Double> rowSumList, rowSumSqList;
			
			for ( PartialSumsWritable val: values ) {
				rowSumList = val.getSum( );
				rowSumSqList = val.getSumSq( );
				
				partialCount += val.getCount( );
				
				if ( partialSumList == null && partialSumSqList == null ) {
					partialSumList = new ArrayList <Double>( rowSumList );
					partialSumSqList = new ArrayList <Double>( rowSumSqList );
				}
				
				else {
					for ( int i = 0; i < rowSumList.size( ); i++ )
						partialSumList.set( i, partialSumList.get( i ) + rowSumList.get( i ) );
					
					for ( int i = 0; i < rowSumSqList.size( ); i++ )
						partialSumSqList.set( i, partialSumSqList.get( i ) + rowSumSqList.get( i ) );
				}
			}
			
			partialSumsValue.setCount( partialCount );
			partialSumsValue.setSum( partialSumList );
			partialSumsValue.setSumSq( partialSumSqList );

			context.write( key, partialSumsValue );
		}
	}
		
	public static class MeanStdDevReduce extends Reducer <NullWritable, PartialSumsWritable, NullWritable, Text> {
		public void reduce( NullWritable key, Iterable <PartialSumsWritable> values, Context context ) throws IOException, InterruptedException {
			Text meansText, stdDevText;
			ArrayList <Double> sumList = null;
			ArrayList <Double> sumSqList = null;
			ArrayList <Double> meansList = null;
			ArrayList <Double> stdDevList = null;
			int count = 0;
			
			ArrayList <Double> partialSumList, partialSumSqList;
			
			StringBuilder sBuilder;
			String prefix;
			double tempVal;

			for ( PartialSumsWritable val: values ) {
				partialSumList = val.getSum( );
				partialSumSqList = val.getSumSq( );
				
				count += val.getCount( );
				
				if ( sumList == null && sumSqList == null ) {
					sumList = new ArrayList <Double>( partialSumList );
					sumSqList = new ArrayList <Double>( partialSumSqList );
				}
				
				else {
					for ( int i = 0; i < partialSumList.size( ); i++ )
						sumList.set( i, sumList.get( i ) + partialSumList.get( i ) );
					
					for ( int i = 0; i < partialSumSqList.size( ); i++ )
						sumSqList.set( i, sumSqList.get( i ) + partialSumSqList.get( i ) );
				}
			}
			
			meansList = new ArrayList <Double>( );
			stdDevList = new ArrayList <Double>( );
			
			for ( int i = 0; i < sumList.size( ); i++ ) {
				meansList.add( sumList.get( i ) / count );
				
				tempVal = sumSqList.get( i ) - count * Math.pow( meansList.get( i ), 2 );
				stdDevList.add( Math.sqrt( tempVal / count ) );
			}
			
			sBuilder = new StringBuilder( );
			prefix = "";
			
			for ( int i = 0; i < meansList.size( ); i++ ) {
				sBuilder.append( prefix ).append( meansList.get( i ) );

				if ( i == 0 )
					prefix = "\t";
			}
			
			meansText = new Text( sBuilder.toString( ) );
			
			sBuilder = new StringBuilder( );
			prefix = "";
			
			for ( int i = 0; i < stdDevList.size( ); i++ ) {
				sBuilder.append( prefix ).append( stdDevList.get( i ) );

				if ( i == 0 )
					prefix = "\t";
			}
			
			stdDevText = new Text( sBuilder.toString( ) );
			
			context.write( key, meansText );
			context.write( key, stdDevText );
		}
	}
	
	public static void main( String args[ ] ) throws Exception {
		Configuration conf = new Configuration( );
		Job job = Job.getInstance( conf );
		
		job.setJobName( "Mean and Std Deviation" );
		job.setJarByClass( MeanStdDev.class );
		
		job.setMapperClass( MeanStdDevMap.class );
		job.setCombinerClass( MeanStdDevCombine.class );
		job.setReducerClass( MeanStdDevReduce.class );
		job.setNumReduceTasks( 1 );
		
		job.setMapOutputKeyClass( NullWritable.class );
		job.setMapOutputValueClass( PartialSumsWritable.class );
		job.setOutputKeyClass( NullWritable.class );
		job.setOutputValueClass( Text.class );

		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		
		Path in = new Path( args[ 0 ] );
		Path out = new Path( args[ 1 ] );
		
		FileInputFormat.setInputPaths( job, in );
		FileOutputFormat.setOutputPath( job, out );
		
		job.waitForCompletion( true );
	}
}

