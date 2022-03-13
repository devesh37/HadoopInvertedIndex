package com.hadoopproject.invertedindex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Map extends Mapper<LongWritable, Text, Text, Text>{
	  private Text word = new Text();
	    private Text filename = new Text();
	  //inp=(line_number,'hello world-.devesh')--->map---> out=[('hello','file_name'),('world','file_name'),('devesh','file_name')]
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		FileSplit currentBlock = ((FileSplit) context.getInputSplit());
		String filename_str=currentBlock.getPath().getName();
		String line=value.toString();
		filename=new Text(filename_str);
		line = line.replaceAll("[^a-zA-Z0-9]+"," ").toUpperCase();//to remove special char
		
        StringTokenizer words = new StringTokenizer(line);
        
        while (words.hasMoreTokens()) {
            word.set(words.nextToken());
            context.write(word, filename);
        }
	}
	
}
