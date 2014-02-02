package com.cloudwick.uniqueurl;



	import java.io.IOException;
import java.util.HashSet;

	import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	/**
	 * @author Harsha
	 *
	 */
	public class UUReducer extends Reducer<Text, Text, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<Text> valList, Context context)
				throws IOException, InterruptedException {
			
			HashSet<String> hs = new HashSet<String>();
			
			for(Text val:valList) {
				hs.add(val.toString());
				
			}
			
			context.write(key, new IntWritable(hs.size()));
		}
		
		
	}

