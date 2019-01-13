package buildCluster;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class loadLevelReducer extends MapReduceBase
implements Reducer<Text, Text, Text, Text>
{

	public void reduce(Text _key, Iterator values,
			OutputCollector output, Reporter reporter)
	throws IOException 
	{
		while (values.hasNext()) 
		{
			output.collect(_key, (Text)values.next());
		}
	}

}

