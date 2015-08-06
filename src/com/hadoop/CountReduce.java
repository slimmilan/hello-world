/**
 * 
 */
package com.hadoop;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * @author milan
 * 
 */
public class CountReduce extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, IntWritable> {

	@SuppressWarnings("unchecked")
	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		for (IntWritable count : (Iterable<IntWritable>) values) {
			int sum = 0;
			sum = sum + count.get();
			output.collect(key, new IntWritable(sum));
		}
	}

}
