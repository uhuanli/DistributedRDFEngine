package summaryGraph;

import hbase.myHbase;

import java.io.IOException;
import java.util.Iterator;

import mapreduce.offlineDriver;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class loadSumEdgeReducer extends MapReduceBase
implements Reducer<Text, Text, Text, Text>
{

	public void reduce(Text _key, Iterator values,
			OutputCollector output, Reporter reporter)
	throws IOException 
	{
		//l_subID_level => objID \t eSig \t number
		//o_objID_level => subID \t subSig \t eSig \t number
		int _count = 0;
		String[] __sp = _key.toString().split("_");
		int _last = 0;
		String _val = null;
		String _pre = null;
		{
			if(__sp[0].charAt(0) == 'l') _pre = "l";
			if(__sp[0].charAt(0) == 's') _pre = "out";
			if(__sp[0].charAt(0) == 'o') _pre = "in";
		}
		String _qualifier = null;
		int _level = Integer.parseInt(__sp[2]);
		String _family = offlineDriver.levelFamily[_level];
		while (values.hasNext()) 
		{
			_val = ((Text)values.next()).toString();
			_last = _val.lastIndexOf("\t");
			_qualifier = _pre + _val.substring(_last+1);// l0  in0  out0
			myHbase.loadSumEdgeValue(__sp[1], _family, _qualifier, _val.substring(0, _last));
			_count ++;
		}
		//load size
		_qualifier = _pre + "sz";
		myHbase.loadSumEdgeValue(__sp[1], _family, _qualifier, _count + "");
	}

}
