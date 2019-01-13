package summaryGraph;

import hbase.myHbase;

import java.io.IOException;

import mapreduce.globalConf;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.kenai.jaffl.annotations.In;

public class loadSumEdgeMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>
{
	globalConf _db = globalConf.Yago2;
	private Text tKey = new Text(); 
	private Text tVal = new Text(); 
	StringBuilder _sb = new StringBuilder();
	public void map(LongWritable key, Text values,
			OutputCollector<Text, Text> output, Reporter reporter)
	throws IOException 
	{
		 if(_db.equals(globalConf.Yago)){
			 
		 }
		 else
		 if(_db.equals(globalConf.Yago2))
		 {
			 loadSumEdge_yago2(key, values, output, reporter);
		 }
	}
	private void loadSumEdge_yago2(LongWritable key, Text values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException
	{
		//l_subID_level \t objID \t eSig \t number
		//o_objID_level \t subID \t subSig \t eSig \t number
		String _line = values.toString();
		int first_tab = _line.indexOf("\t");
		tKey.set(_line.substring(0, first_tab));
		tVal.set(_line.substring(first_tab+1));
		output.collect(tKey, tVal);
	}
	private int getLevelFromString(String _str){
		int _last = _str.lastIndexOf("_");
		int _level = Integer.parseInt(_str.substring(_last + 1));
		return _level;
	}
}