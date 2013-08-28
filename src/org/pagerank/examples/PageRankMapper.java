package org.pagerank.examples;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper  
       extends Mapper<Object, Text, Text, Text>{
        
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] part = value.toString().split("\t");
      String node = part[0];
      String[] split = part[1].split(":");
      String oldScore = split[0];
      String outlink = split[1];
      
      if(outlink.startsWith("D")){ 
    	  
      } else {
	      String[] outNode = outlink.split(",");
	      float foldScore = Float.parseFloat(oldScore)/outNode.length;
	      
	      for (int i = 0; i < outNode.length;i++){
	    	  context.write(new Text(outNode[i]),new Text(String.valueOf(foldScore)));
	      }
      }

      context.write(new Text(node),new Text(":" + outlink));
    }
  }
