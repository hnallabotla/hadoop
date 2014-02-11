package com.cloudwick.secondarysort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SecondarySortReducer extends
		Reducer<CustomWritable, Text, Text, Text> {

	@Override
	protected void reduce(CustomWritable key, Iterable<Text> valList,
			Context context) throws IOException, InterruptedException {

		ArrayList<Text> arr = new ArrayList();
		String output = null;

	/*	for (Text val : valList) {

			arr.add(val);
		}
		for (int i = 1; i < arr.size(); i++) {
			output = arr.get(0).toString() + arr.get(i).toString();
		}*/
		Iterator<Text> it = valList.iterator();
		while(it.hasNext())
		context.write(key.getFirst(), it.next());
	}

}