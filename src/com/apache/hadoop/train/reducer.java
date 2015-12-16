package com.apache.hadoop.train;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class reducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	@Override
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException
	{
		int sum=0;
		//int c=0;
		
		for(IntWritable value:values )
		{
			int x=value.get();
			sum=sum+x;
		}
		
		context.write(key, new IntWritable(sum));
		}

	}	
