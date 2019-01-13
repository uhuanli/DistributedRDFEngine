package addID2yago;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class addLineIDMapper extends MapReduceBase
implements Mapper<LongWritable, Text, IntWritable, Text>
{
	private int _line_id = 0;
	private static IntWritable iKey = new IntWritable();
	private static Text tVal = new Text();
	public void map(LongWritable key, Text values,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
	throws IOException 
	{
		 
	}

}
