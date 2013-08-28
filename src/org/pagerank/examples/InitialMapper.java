package org.pagerank.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class InitialMapper 
     extends Mapper<Object, Text, Text, Text>{
	  
  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
  	String[] part = value.toString().split(":");
      String node = part[0];
      StringTokenizer outlink = new StringTokenizer(part[1]);
      String outLinkStr = "";
      while(outlink.hasMoreTokens()){
	        String val = outlink.nextToken();
	      	context.write(new Text(val),new Text(node));
	      	outLinkStr += val + ",";
      }
      context.write(new Text(node),new Text("1.0" + ":" + outLinkStr.substring(0,outLinkStr.length()-1)));
    }
  }

