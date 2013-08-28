package org.pagerank.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class pagerank {	
    public static void main(String[] args) throws Exception {
    	int ret = 0;
    	long pre = (System.currentTimeMillis());
    	ret = initial();
    	long post = (System.currentTimeMillis());
    	System.out.println("Time: " + (post - pre));
    	for(int i = 0;i < Constant.Itr;i++){
        	pre = (System.currentTimeMillis());
    		ret = pagerankcalc();
        	post = (System.currentTimeMillis());
        	System.out.println("Time: " + (post - pre));
    	}
    	pre = (System.currentTimeMillis());
    	ret = sort();
    	post = (System.currentTimeMillis());
    	System.out.println("Time: " + (post - pre));
    	System.exit(ret);
    }
    
	public static int initial() throws Exception{
		Configuration conf = new Configuration();
	    //conf.set("mapred.job.tracker", "59.78.43.167:54311");
	    String[] otherArgs = {"/user/graph","/user/graph-in"};

	    Job job = new Job(conf, "Page Rank Initial");
	    job.setJarByClass(pagerank.class);
	    job.setMapperClass(InitialMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setCombinerClass(InitialCombiner.class);
	    job.setReducerClass(InitialReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(4);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    int ret = (job.waitForCompletion(true) ? 0 : 1);
	    return ret;
	}
	
	@SuppressWarnings("deprecation")
	public static int pagerankcalc() throws Exception{
		// deal with the dead end
	    Configuration conf = new Configuration();
	    //conf.set("mapred.job.tracker", "59.78.43.167:54311");
	    FileSystem fs = FileSystem.get(conf); 
	    if(fs.exists(new Path("/user/deadend"))){
	    	fs.delete(new Path("/user/deadend"),true);
	    }
	    String[] otherArgs = {"/user/graph-in","/user/deadend"};
	    Job job = new Job(conf, "Dead End");
	    job.setJarByClass(pagerank.class);
	    job.setMapperClass(DeadEndMapper.class);
	    job.setReducerClass(DeadEndReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    int ret = (job.waitForCompletion(true) ? 0 : 1);
	    String str = null;
	    if(ret == 0){
	    	FSDataInputStream in = fs.open(new Path("/user/deadend/part-r-00000"));
	    	str = in.readLine();
	    	System.out.println(str);
	    }
	    String[] part = str.split("\t");
	    
	    // calculate the page rank
		 conf = new Configuration();
		 //conf.set("mapred.job.tracker", "59.78.43.167:54311");
		 conf.set("DEAD END SCORE", part[1]);
	     String[] pathArgs = {"/user/graph-in","/user/graph-out"};

		 job = new Job(conf, "Page Rank");
		 job.setJarByClass(pagerank.class);
		 job.setMapperClass(PageRankMapper.class);
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(Text.class);
		 job.setCombinerClass(PageRankCombiner.class);
		 job.setReducerClass(PageRankReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 job.setNumReduceTasks(4);
		 FileInputFormat.addInputPath(job, new Path(pathArgs[0]));
		 FileOutputFormat.setOutputPath(job, new Path(pathArgs[1]));
		 ret = (job.waitForCompletion(true) ? 0 : 1);
		 if(ret == 0){
 			fs.delete(new Path("/user/graph-in"),true);
 			fs.rename(new Path("/user/graph-out"), new Path("/user/graph-in"));
 		 }
		 return ret;
	}
	public static int sort() throws Exception{
		Configuration conf = new Configuration();
	    //conf.set("mapred.job.tracker", "59.78.43.167:54311");
	    String[] otherArgs = {"/user/graph-in","/user/graph-out"};

	    Job job = new Job(conf, "Page Rank Sort");
	    job.setJarByClass(pagerank.class);
	    job.setMapperClass(PageRankSortMapper.class);
	    job.setMapOutputKeyClass(FloatWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    int ret = (job.waitForCompletion(true) ? 0 : 1);
	    return ret;
	}
}

