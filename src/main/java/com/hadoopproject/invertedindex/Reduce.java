package com.hadoopproject.invertedindex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text,Text, Text> {
	
	@Override
	public void reduce(final Text key,final Iterable<Text> valueList,final Context context) throws IOException, InterruptedException 
	{
		Iterable<Text> filenameList=valueList;
		 StringBuilder stringBuilder = new StringBuilder();
		 
	        for (Text value : filenameList) {
	            stringBuilder.append(value.toString());
	       
	            if (filenameList.iterator().hasNext()) {
	                stringBuilder.append(",");
	            }
	        }

	        context.write(key, new Text(stringBuilder.toString()));
	
	}

}
