import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 


public class AssignMapper extends Mapper<LongWritable, Text,Text,IntWritable> {
	IntWritable one = new IntWritable(1);
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] data=value.toString().split(Pattern.quote("|"));
		Text state=new Text(data[3]);
		if(!(data[0].equals("NA")||data[1].equals("NA")))
		{
			if(data[0].equals("Onida"))
		context.write(state,one);
		}
	}
}