package addID2yago;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class addID2yagoReducer extends MapReduceBase
implements Reducer<IntWritable, Text, IntWritable, Text>
{

	public void reduce(IntWritable _key, Iterator values,
			OutputCollector output, Reporter reporter)
	throws IOException 
	{
		while (values.hasNext()) 
		{
			output.collect(_key, (Text)values.next());
		}
	}

}
