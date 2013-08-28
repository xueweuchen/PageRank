package org.pagerank.examples;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class InitialCombiner extends Reducer<Text, Text , Text, Text> {
	  public void reduce(Text key, Iterable<Text> values, 
              Context context
              ) throws IOException, InterruptedException {
		  boolean hasinlink = false;
		  for (Text val : values) {
			 if(val.toString().startsWith("1.0")){
				  context.write(key, val);
			 } else {
				 hasinlink = true;
			 }
			}
		  if(hasinlink = true){
			  context.write(key, new Text("Y"));
		  }
		}
}
