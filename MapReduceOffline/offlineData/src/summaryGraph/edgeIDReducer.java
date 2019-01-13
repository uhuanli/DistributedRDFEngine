package summaryGraph;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class edgeIDReducer extends MapReduceBase
implements Reducer<Text, Text, Text, Text>
{
	Text tVal = new Text();
	public void reduce(Text _key, Iterator values,
			OutputCollector output, Reporter reporter)
	throws IOException 
	{
		int _count = 0;
		while (values.hasNext()) 
		{
			String _val = ((Text)values.next()).toString();
			tVal.set(_val + "\t" + _count);
			output.collect(_key, tVal);
			_count ++;
		}
	}

}
