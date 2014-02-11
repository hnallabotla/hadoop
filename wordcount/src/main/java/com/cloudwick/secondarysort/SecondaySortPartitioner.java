package com.cloudwick.secondarysort;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondaySortPartitioner extends Partitioner<CustomWritable, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	@Override
	public int getPartition(CustomWritable cw, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		return Math.abs(Integer.parseInt(cw.getFirst().toString())*127)%numPartitions;
	}
	

}
