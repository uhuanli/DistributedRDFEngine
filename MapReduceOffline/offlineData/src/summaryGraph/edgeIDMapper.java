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

public class edgeIDMapper extends MapReduceBase
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
			 edgeID_yago2(key, values, output, reporter);
		 }
	}
	private void edgeID_yago2(LongWritable key, Text values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException
	{
		//l_subID_level \t objID \t eSig
		//o_objID_level \t subID \t eSig
		//o_subID_level \t objID \t eSig
		String[] _sp = values.toString().split("\t");
		tKey.set(_sp[0]);
		_sb.setLength(0);
		if(_sp[0].charAt(0) == 'l'){
			_sb.append(_sp[1]).append("\t").append(_sp[2]);
			tVal.set(_sb.toString());
		}
		else 
		{
			int _level = getLevelFromString(_sp[0]);
			int _id = Integer.parseInt(_sp[1]);
			String _str_sig = myHbase.getVSignature(_id, _level);
			_sb.append(_sp[1]).append("\t").append(_str_sig)
			   .append("\t").append(_sp[2]);
			//subID \t subSig \t eSig
			tVal.set(_sb.toString());
		}
		output.collect(tKey, tVal);
	}
	private int getLevelFromString(String _str){
		int _last = _str.lastIndexOf("_");
		int _level = Integer.parseInt(_str.substring(_last + 1));
		return _level;
	}
}