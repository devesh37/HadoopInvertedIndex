package com.hadoopproject.invertedindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class InvertedIndexJobImpl extends Configured implements Tool {
	
	public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length!=2)
		{
			System.out.println("Invalid args! Args Length=2");
			return -1;
		}
	//step 1: Create job object
		Job job = Job.getInstance(getConf());
	//step 2: set inp/out path
		Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);
        FileInputFormat.setInputDirRecursive(job, true); //sub dir read
    //step 3:Setting name and jar
        job.setJobName("InvertedIndex");
        job.setJarByClass(InvertedIndexJobImpl.class);
    //step 4: Configuration
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
    //step 5: All logical classes
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        //job.setCombinerKeyGroupingComparatorClass();
        job.setNumReduceTasks(2);
        //job.setPartitionerClass();
        //job.setSortComparatorClass();
        //job.setGroupingComparatorClass();
        job.setReducerClass(Reduce.class);
	//step 6: Set Output Type
        //Mapper Output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //Reducer Output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		
        return job.waitForCompletion(true) ? 0 : 1;
		
		
	}

}
