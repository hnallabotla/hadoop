package com.cloudwick.secondarysort;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortKeyComparator extends WritableComparator {

	protected SecondarySortKeyComparator() {
		super(CustomWritable.class, true);
		// TODO Auto-generated constructor stub
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.
	 * WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		CustomWritable cw = (CustomWritable) a;
		CustomWritable cw2 = (CustomWritable) b;
		int cmp = a.compareTo(b);
		return cmp;
	}

}
