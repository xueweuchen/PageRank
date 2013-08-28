package org.pagerank.examples;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InitialReducer 
 extends Reducer<Text,Text,Text,Text> {

	  public void reduce(Text key, Iterable<Text> values, 
	                     Context context
	                     ) throws IOException, InterruptedException {
	    boolean isDeadEnd = true;
	    for (Text val : values) {
	  	  if(val.toString().startsWith("1.0")){
	  		  isDeadEnd = false;
	  		  context.write(key, val);
	  	  }
	    }
	    if(isDeadEnd == true){
	  	  context.write(key, new Text("1.0:D"));
	    }
	  }
	}
