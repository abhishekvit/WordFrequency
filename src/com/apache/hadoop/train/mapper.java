package com.apache.hadoop.train;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class mapper extends
Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable mapperInputKey, Text mapperInputValue,
			Context context) throws IOException, InterruptedException 
	{
		String line = mapperInputValue.toString();
		String[] values = line.split(" ");
		for(String value : values)
		{
			context.write(new Text(value), new IntWritable(1));
		}
		
		
	}

}
