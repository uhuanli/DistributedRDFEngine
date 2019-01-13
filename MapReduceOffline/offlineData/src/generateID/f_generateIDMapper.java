package generateID;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class f_generateIDMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>
{
	Text mpVal = new Text();
	Text mpKey = new Text();
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
	throws IOException 
	{
		{
//			_ii ++;
//			if(_ii % 10000 != 0) return;
		}
		String[] _sp = (value.toString()).split("\t");
		mpVal.set(_sp[0]);

		for(int i = 1; i < _sp.length; i++)
		{
			mpKey.set(_sp[i]);
			output.collect(mpKey, mpVal);
		}
	}
	private int _ii = 0;
}