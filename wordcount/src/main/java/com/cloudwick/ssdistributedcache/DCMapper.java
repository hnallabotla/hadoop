package com.cloudwick.ssdistributedcache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DCMapper extends Mapper<LongWritable, Text, Text, Text> {

	BufferedReader br = null;
	String curLine;
	
	HashMap<String, String> deptHashMap = new HashMap<String, String>();

	protected void setup() {

		try {

			br = new BufferedReader(new FileReader("dept"));

			while ((curLine = br.readLine()) != null) {
				System.out.println(curLine);
				String[] deptWords = curLine.split(",");
				deptHashMap.put(deptWords[0], curLine);
			}

		

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(",");

		// Spliting dept record to words

		if (deptHashMap.containsKey(words[2].trim())) {

			String val = deptHashMap.get(words[2]);
			
			context.write(new Text(words[2]), new Text(val));
		}

	}

}
