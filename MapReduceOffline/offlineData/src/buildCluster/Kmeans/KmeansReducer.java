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
import org.apache.hadoop.mapred.jobconf_005fhistory_jsp;

public class KmeansReducer extends MapReduceBase
implements Reducer<Text, Text, Text, Text>
{
	private ArrayList<mCenter> ctr2subList;
	private Text tKey = new Text();
	private Text tVal = new Text();
	private int subNum_center = -1;
	public void reduce(Text _key, Iterator values,
			OutputCollector output, Reporter reporter)
	throws IOException 
	{
		ctr2subList = new ArrayList<mCenter>();
		while (values.hasNext()) 
		{
			ctr2subList.add(new mCenter(((Text)values.next()).toString()));
		}
		{
			if(ctr2subList.size() < 40) reporter.incrCounter(oReport.lt40, 1);
			if(ctr2subList.size() < 10) reporter.incrCounter(oReport.lt10, 1);
			if(ctr2subList.size() > 1000) reporter.incrCounter(oReport.gt1000, 1);
			if(ctr2subList.size() > 2000) reporter.incrCounter(oReport.gt2000, 1);
			
			if(ctr2subList.size() > 2 * subNum_center)
			{
				reporter.incrCounter(oReport.gtThres, 1);
			}
			if(ctr2subList.size() < subNum_center / 2)
			{
				reporter.incrCounter(oReport.ltThres, 1);
			}
			reporter.incrCounter(oReport.CenterNum, 1);
		}
		mCenter _m = newCenter(ctr2subList);
		{
			// calculator the change dist of the old center and the new one 
			for(mCenter iCenter : ctr2subList)
			{
				if(iCenter.getSub().equals(_key.toString()))
				{
					reporter.incrCounter(oReport.TotalReduceDist, _m.dist(iCenter));
					reporter.incrCounter(oReport.nTotalReduceDist, 1);
					break;
				}
			}
		}
		tKey.set(_m.getSub());
		tVal.set(_m.getSig().toString() + "\t" + ctr2subList.size());
		output.collect(tKey, tVal);
	}
	public void configure(JobConf _job)
	{
		Configuration _jcf = _job;
		subNum_center = (int) _jcf.getLong("subNum_center", -1);
	}
	public mCenter newCenter(ArrayList<mCenter> _ctr_list)
	{
		mCenter _ret = null;
		int[] cVector = new int[Signature.getLength()];
		{
			for(int i = 0; i < cVector.length; i ++)
				cVector[i] = 0;
		}
		for(mCenter iCenter : _ctr_list)
		{
			int[] _c = iCenter.getSigVector();
			{
				for(int i = 0; i < cVector.length; i ++)
				{
					cVector[i] += _c[i];
				}
			}
		}
		/*
		 * similarity : sum(value whose relative byte is 1) - sum(those is 0);
		 */
		long similarity = -(offlineDriver.SubScale * (Signature.length + 1));
		for(mCenter iCenter : _ctr_list)
		{
			int _tmp = 0;
			int [] _c = iCenter.getSigVector();
			{
				for(int i = 0; i < cVector.length; i ++)
				{
					// for mapping [1 to 1] and [0 to -1]
					_tmp += (_c[i] * 2 - 1) * cVector[i];
				}
				if(_tmp > similarity)
				{
					similarity = _tmp;
					_ret = iCenter;
				}
			}
		}
		
		return _ret;
	}
}
