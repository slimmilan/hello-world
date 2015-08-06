/**
 * 
 */
package com.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * @author milan
 * 
 */
public class CountMap extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter report)
			throws IOException {
		String[] word = value.toString().split(" ");
		for (String str : word) {
			output.collect(new Text(str), new IntWritable(1));
		}
	}
}
