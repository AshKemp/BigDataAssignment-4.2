package mapreduce.demo.task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>
{	
	Text minMaxVal;
	
	@Override
	public void setup(Context context) {
		minMaxVal = new Text();
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int maxVal = Integer.MIN_VALUE;
		int minVal = Integer.MAX_VALUE;
		
		for (IntWritable value : values) {
			if (value.get() > maxVal) {
				maxVal = value.get();
			}
			if (value.get() < minVal) {
				minVal = value.get();
			}
		}
		
		minMaxVal.set("Max Temp: " + maxVal + ", Min Temp: " + minVal);
		context.write(key, minMaxVal);
	}
}
