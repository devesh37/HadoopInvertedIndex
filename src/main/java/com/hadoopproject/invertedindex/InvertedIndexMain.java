package com.hadoopproject.invertedindex;

import org.apache.hadoop.util.ToolRunner;


public class InvertedIndexMain {

	public static void main(String[] args) throws Exception {
	    
		int exitCode = ToolRunner.run(new InvertedIndexJobImpl(), args);
		System.exit(exitCode);
	}

}
