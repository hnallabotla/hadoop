package com.cloudwick.secondarysort;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class SecondarySortDeptMappper extends Mapper<LongWritable, Text, CustomWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(",");
		CustomWritable cw = new CustomWritable(words[0],"1");
		
		
		//	if(words[0].length() > 0&&words[1].length() > 0)
				context.write(cw,value);
		
		
	}
}
