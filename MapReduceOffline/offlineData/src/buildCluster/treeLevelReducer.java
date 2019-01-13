package buildCluster;

import hbase.myHbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import mapreduce.Signature;
import mapreduce.oReport;
import mapreduce.offlineDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.jruby.RubyProcess.Sys;

import buildCluster.Kmeans.mCenter;

public class treeLevelReducer extends MapReduceBase
implements Reducer<Text, Text, Text, Text>
{
	private ArrayList<mCenter> ctr2subList;
	private int level = -1;
	private Text tKey = new Text();
	private Text tVal = new Text();
	public void reduce(Text _key, Iterator values,
			OutputCollector output, Reporter reporter)
	throws IOException 
	{
		ctr2subList = new ArrayList<mCenter>();
		while (values.hasNext()) 
		{
			ctr2subList.add(new mCenter(((Text)values.next()).toString()));
		}
		int _parts = ctr2subList.size()/offlineDriver.nodeThreshold + 1;
		int _num_part = ctr2subList.size()/_parts + 1;
		for(int i = 0; i < _parts; i ++)
		{
			int _end = Math.min(_num_part * (i + 1), ctr2subList.size());
			int _begin = _num_part * i;
			
			mCenter _mPart = partCenter(ctr2subList, _begin, _end);
			mCenter _m = null;
			{
				Signature _tmp = new Signature();
				for(int j = _begin; j < _end; j ++)
				{
					_tmp.or(ctr2subList.get(j).getSig());
				}
				_m = new mCenter(_mPart.getSub(), _tmp);
			}
			{
				if(myHbase.admin == null)
				{
					reporter.incrCounter(oReport.adminNULL, 1);
				}
			}
			StringBuffer _sb = new StringBuffer();
			{
				reporter.incrCounter(oReport.CenterNum, 1);
				reporter.incrCounter(oReport.totalSon, (_end - _begin));
//				myHbase.submitCluster(_m, ctr2subList, _begin, _end, level);
				{
					_sb.append(ctr2subList.get(_begin).toString());
					for(int i1 = _begin + 1; i1 < _end; i1 ++)
					{
						_sb.append("|").append(ctr2subList.get(i1).toString());
					}
				}
			}
			tKey.set(_m.getSub());
			tVal.set(_m.getSig().toString() + "|" + _sb.toString());
			output.collect(tKey, tVal);
		}
		ctr2subList = null;
	}
	private int _ii = 0;
	public void configure(JobConf _job)
	{
		Configuration _conf = _job;
		level = (int)_conf.getLong("level", -1);
	}
	public static boolean checkOR(Signature _s, ArrayList<mCenter> _c_list, int _b, int _e)
	{
		Signature new_s = new Signature();
		for(int i = _b; i < _e; i ++)
		{
			new_s.or(_c_list.get(i).getSig());
		}
		if(! new_s.sigEqual(_s)) return false;
		return true;
	}
	public mCenter partCenter(ArrayList<mCenter> _ctr_list, int _begin, int _end)
	{
		mCenter _ret = null;
		int[] cVector = new int[Signature.getLength()];
		{
			for(int i = 0; i < cVector.length; i ++)
				cVector[i] = 0;
		}
		for(int ic = _begin; ic < _end; ic ++)
		{
			mCenter iCenter = _ctr_list.get(ic);
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
		for(int ic = _begin; ic < _end; ic ++)
		{
			mCenter iCenter = _ctr_list.get(ic);
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
