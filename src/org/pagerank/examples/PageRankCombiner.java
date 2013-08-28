package org.pagerank.examples;

import java.io.IOException;

//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankCombiner extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, 
            Context context
            ) throws IOException, InterruptedException {
		String outlink = null;
		float newRank = 0;
		for (Text val : values){
			String valstr = val.toString();
			if(valstr.startsWith(":")){
				  outlink = valstr;
			} else {
					  float oldScore = Float.valueOf(val.toString());
					  newRank += (oldScore);
			}
		}
		context.write(key,new Text(String.valueOf(newRank)));
		if(outlink != null)
			context.write(key, new Text(outlink));
	}
}
