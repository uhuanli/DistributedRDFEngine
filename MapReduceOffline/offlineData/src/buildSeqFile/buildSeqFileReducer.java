package buildSeqFile;

import java.io.IOException;
import java.util.Iterator;

import mapreduce.oReport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class buildSeqFileReducer extends MapReduceBase 
implements Reducer<Text, Text, Text, Text> 
{

	@Override
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) 
	throws IOException 
	{
		while(values.hasNext())
		{
			output.collect(_key, (Text)(values.next()));
		}
		reporter.incrCounter(oReport.SubNumber, 1);
	}

	int _ii = 0;
}
