package com.cloudwick.secondarysort;



import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudwick.uniqueurl.UUDriver;
import com.cloudwick.uniqueurl.UUMapper;
import com.cloudwick.uniqueurl.UUReducer;

public class SecondarySortDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length != 3) {
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", 
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
            return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(SecondarySortDriver.class);
		job.setJobName(getClass().getName());
		
	//	FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,SecondarySortEmpMappper.class );
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,SecondarySortDeptMappper.class );
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		
		job.setPartitionerClass(SecondaySortPartitioner.class);
		job.setSortComparatorClass(SecondarySortKeyComparator.class);
		job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
		job.setReducerClass(SecondarySortReducer.class);

		
		job.setMapOutputKeyClass(CustomWritable.class);
		//job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		
	//	job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		if(job.waitForCompletion(true)) {
			return 0;
		}
		return 1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new SecondarySortDriver(), args));
		
	}

}
