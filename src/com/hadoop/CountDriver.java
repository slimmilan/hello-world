/**
 * 
 */
package com.hadoop;

import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;



/**
 * @author milan
 *
 */
public class CountDriver{

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(CountDriver.class);
		conf.setJobName("Word Count");
		conf.setMapperClass(CountMap.class);
		conf.setReducerClass(CountReduce.class);
		FileInputFormat.addInputPath(conf, new Path( "C:\\eclipse\\eclipse-jee-indigo-SR2-win32-x86_64\\workspaces\\WordCount\\input"));
		FileOutputFormat.setOutputPath(conf, new Path("C:\\eclipse\\eclipse-jee-indigo-SR2-win32-x86_64\\workspaces\\WordCount\\output"));
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(conf);
	}

}
