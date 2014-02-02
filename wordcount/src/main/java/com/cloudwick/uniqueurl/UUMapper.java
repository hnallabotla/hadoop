package com.cloudwick.uniqueurl;



import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UUMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(",");
		
			if(words[0].length() > 0&&words[1].length() > 0)
				context.write(new Text(words[0]), new Text(words[1]));
		
		
	}
}
