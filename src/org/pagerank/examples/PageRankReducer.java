package org.pagerank.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      String outlink = null;
      Configuration conf = context.getConfiguration();
      float deadEndScore = Float.parseFloat(conf.get("DEAD END SCORE"));
      float newRank = 0;
      float beta = 0.85F;
      for (Text val : values){
    	  String valstr = val.toString();
    	  if(valstr.startsWith(":")){
    		  outlink = valstr;
    	  } else {
    		  float oldScore = Float.valueOf(val.toString());
    		  newRank += (oldScore);
    	  }
      }
      newRank = beta * newRank + (1 - beta) + beta * deadEndScore;
      context.write(key, new Text(newRank + outlink));
    }
  }
