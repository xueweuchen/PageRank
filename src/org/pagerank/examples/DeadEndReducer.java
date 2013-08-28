package org.pagerank.examples;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeadEndReducer 
extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> values, 
	                Context context
	                ) throws IOException, InterruptedException {
		float sum = 0.0F;
		if(key.toString().startsWith("D")){
		   for (Text val : values) {
		 	float valf = Float.valueOf(val.toString());
		     sum += valf;
		   }
		}
		sum = sum/Constant.NodeSize;
		context.write(key, new Text(Float.toString(sum)));
	}
}
