package buildCluster.Kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import mapreduce.Signature;
import mapreduce.oReport;
import mapreduce.offlineDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.sun.org.apache.bcel.internal.generic.NEW;

public class findCenterReducer extends MapReduceBase
implements Reducer<Text, Signature, Text, Text> 
{
	private static ArrayList<mCenter> ctrList = new ArrayList<mCenter>();
	private static Text emValText = new Text();
	private static int numCenter = 50*1000;
	public void reduce(Text _key, Iterator values,
			OutputCollector output, Reporter reporter)
	throws IOException 
	{
		if(ctrList.size() > numCenter) return;
		while (values.hasNext()) 
		{
			/*
			 * check whether multiple signature
			 */
			Signature _sig = (Signature)(values.next());
			boolean _exist = false;
			for(mCenter iCenter : ctrList)
			{
				if((iCenter.getSig()).dist(_sig) < offlineDriver.SigCloseDst)
				{
					_exist = true;
					break;
				}
			}
			if(_exist)
			{
				continue;
			}
			{
				reporter.incrCounter(oReport.Center, 1);
			}
			Signature _tmp_sig = new Signature(_sig.toString());
			ctrList.add(new mCenter(_key.toString(), _tmp_sig));
			emValText.set(_sig.toString());
			_sig = null;
			output.collect(_key, emValText);
		}
	}
	public void configure(JobConf _job)
	{
		Configuration _conf = _job;
		numCenter = (int) _conf.getLong("centerNum", -1);//54987
	}
	public int _ii = 0;

}
