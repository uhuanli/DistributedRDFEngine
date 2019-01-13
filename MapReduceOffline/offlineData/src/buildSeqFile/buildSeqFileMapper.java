package buildSeqFile;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class buildSeqFileMapper extends MapReduceBase 
implements Mapper<LongWritable, Text, Text, Text> 
{
	Text emKey = new Text();
	Text emVal = new Text();
	public void map(LongWritable key, Text values,
			OutputCollector<Text, Text> output, Reporter reporter) 
	throws IOException 
	{
		String[] _sp = (values.toString()).split("\t");
		emKey.set(_sp[0]);
		String _val = _sp[1];
		for(int i = 2; i < _sp.length; i ++)
		{
			_val += "\t" + _sp[i];
		}
		emVal.set(_val);
		output.collect(emKey, emVal);
	}
	int _ii = 0;

}
