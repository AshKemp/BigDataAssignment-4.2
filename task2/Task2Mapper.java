package mapreduce.demo.task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task2Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
	IntWritable zip;
	IntWritable temp;
	
	@Override
	public void setup(Context context) {
		zip = new IntWritable();
		temp = new IntWritable();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split(",");
		
		zip.set(Integer.parseInt(lineArray[1]));
		temp.set(Integer.parseInt(lineArray[2]));
		
		context.write(zip, temp);
	}
}
