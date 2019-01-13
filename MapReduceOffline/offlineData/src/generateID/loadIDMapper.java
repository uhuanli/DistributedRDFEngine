package generateID;

import hbase.myHbase;

import java.io.IOException;

import mapreduce.oReport;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class loadIDMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>
{

	public void map(LongWritable key, Text values,
			OutputCollector<Text, Text> output, Reporter reporter)
	throws IOException 
	{
		 String[] _sp = values.toString().split("\t");
		 int _id = Integer.parseInt(_sp[2]);
		 String _str = _sp[1];
		 if(_sp[0].equals("s")){
			 myHbase.setSub2ID(_str, _id);
			 myHbase.setID2Sub(_id, _str);
			 reporter.incrCounter(oReport.SubNumber, 1);
		 }
		 else 
		 if(_sp[0].equals("p")){
			myHbase.setPre2ID(_str, _id);
			myHbase.setID2Pre(_id, _str);
			reporter.incrCounter(oReport.PRENumber, 1);
		 }
		 else
		 if(_sp[0].equals("o")){
			 myHbase.setObj2ID(_str, _id);
			 myHbase.setID2Obj(_id, _str);
			 reporter.incrCounter(oReport.OBJNumber, 1);
		 }
		 else
		 if(_sp[0].equals("l")){
			 myHbase.setLoc2ID(_str, _id);
			 myHbase.setID2Loc(_id, _str);
			 reporter.incrCounter(oReport.LOCNumber, 1);
		 }
		 _sp = null;
	}
}