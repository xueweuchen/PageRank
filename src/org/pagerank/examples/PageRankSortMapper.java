package org.pagerank.examples;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankSortMapper  
extends Mapper<Object, Text, FloatWritable, Text>{
 
public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
		String[] part = value.toString().split("\t");
		String node = part[0];
		String[] split = part[1].split(":");
		FloatWritable fscore = new FloatWritable(Float.parseFloat(split[0]));
		context.write(fscore,new Text(node));
	}
}
