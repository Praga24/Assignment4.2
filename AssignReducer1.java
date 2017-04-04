import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class AssignReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		IntWritable outValue = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int s = 0;
			for (IntWritable value : values) {
				s += value.get();
			}
			outValue.set(s);
			context.write(key, outValue);
		}
	
}
