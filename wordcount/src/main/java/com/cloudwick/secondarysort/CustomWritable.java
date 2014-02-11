package com.cloudwick.secondarysort;


import java.io.*;

import org.apache.hadoop.io.*;

public class CustomWritable implements WritableComparable<CustomWritable> {

	private Text first;
	private Text second;

	public CustomWritable() {
		set(new Text(), new Text());
	}
	public CustomWritable(String s, String s2) {
		set(new Text(s), new Text(s2));
		
	}

	private void set(Text text, Text text2) {
		// TODO Auto-generated method stub
		this.first = text;
		this.second = text2;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);

	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);

	}

	public int compareTo(CustomWritable cw) {
		// TODO Auto-generated method stub
		int cmp = first.compareTo(cw.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(cw.second);

	}

	/**
	 * @return the first
	 */
	public Text getFirst() {
		return first;
	}

	/**
	 * @param first the first to set
	 */
	public void setFirst(Text first) {
		this.first = first;
	}

	/**
	 * @return the second
	 */
	public Text getSecond() {
		return second;
	}

	/**
	 * @param second the second to set
	 */
	public void setSecond(Text second) {
		this.second = second;
	}

}
