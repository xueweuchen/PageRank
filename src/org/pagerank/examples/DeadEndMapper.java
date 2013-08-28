package org.pagerank.examples;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeadEndMapper 
extends Mapper<Object, Text, Text, Text>{

	public void map(Object key, Text value, Context context
	             ) throws IOException, InterruptedException {
		String[] part = value.toString().split("\t");
		String[] split = part[1].split(":");
		if(split[1].startsWith("D")){
			  context.write(new Text("D"), new Text(split[0]));
		}
	}
}
