package summaryGraph;

import hbase.myHbase;

import java.io.IOException;
import java.util.HashMap;

import mapreduce.Signature;
import mapreduce.globalConf;
import mapreduce.oReport;
import mapreduce.offlineDriver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.jruby.RubyProcess.Sys;

public class summaryGraphMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, mEntry>
{
	private static HashMap<Integer, Integer>[] hmFatherCache = null;
	private Signature iSig = new Signature();
	StringBuffer _sb = new StringBuffer();
	private Text tKey = new Text();
	private mEntry entityVal = new mEntry();
	private StringBuffer iBuf = new StringBuffer(); 
	private int subNum = -1;
	private int levelNum = -1;
	private globalConf _db = globalConf.Yago2;
	private String strObjID, strSubID, strPreID, strOBJ;
	public void map(LongWritable key, Text values,
			OutputCollector<Text, mEntry> output, Reporter reporter)
	throws IOException 
	{
		//line: subID, preID, objID, sub, pre, obj
		if(_db.equals(globalConf.Yago))
		{
//			forYago(values, output, reporter);
			forYago_sumEdge(values, output, reporter);
		}
		else
		if(_db.equals(globalConf.Yago2))
		{
			forYago2_sumEdge(values, output, reporter);
		}
	}
	private int _ii = 0;
	private void forYago_sumEdge(Text values, OutputCollector<Text, mEntry> output, Reporter reporter) throws IOException
	{
		iSig.clear();
		String _line = values.toString();
		String [] rdfUnit = _line.split("\t");
		/*
		 * 
		 */		
		int subID = -1, objID = -1, preID = -1;
		{
			objID = Integer.parseInt(rdfUnit[2]);
			subID = Integer.parseInt(rdfUnit[0]);
			preID = Integer.parseInt(rdfUnit[1]);
		}
		/*
		 * 
		 */
		{
			// s_subID_level
			StringBuffer _sb = null;
			int _cur_subID = subID, _cur_objID = objID;
			int i_level = 0;
			if(objID >= subNum)//with literal
			{
				offlineDriver.buildPreSignature(preID, iSig, 1);
				_sb = new StringBuffer();
				_sb.append("l_").append(subID).append("_").append(i_level);
				output.collect(new Text(_sb.toString()), new mEntry(objID, iSig));
				_sb = null;
				{
					reporter.incrCounter(oReport.ledgeNum, 1);
				}
				return;
			}
			offlineDriver.buildPreSignature(preID, iSig, 1);
			offlineDriver.buildEntitySignature(rdfUnit[5], iSig, offlineDriver.hashType, 1);
			{
				reporter.incrCounter(oReport.edgeNum, 1);
			}
			while(true)
			{
				_sb = new StringBuffer("s_").append(_cur_subID).append("_").append(i_level);			
				output.collect(new Text(_sb.toString()), new mEntry(_cur_objID, iSig));
				{// for objID -> subID
					_sb = new StringBuffer("o_").append(_cur_objID).append("_").append(i_level);
					output.collect(new Text(_sb.toString()), new mEntry(_cur_subID, iSig));
				}
				_cur_subID = myHbase.getFather(_cur_subID, i_level);// father of root is -1
				_cur_objID = myHbase.getFather(_cur_objID, i_level);
				{
					if(_cur_subID == -1) break;
				}
				i_level ++;
			}
			_sb = null;
		}
	}
	private void forYago(Text values, OutputCollector<Text, mEntry> output, Reporter reporter) throws IOException
	{
		String _line = values.toString();
		String [] rdfUnit = _line.split("\t");
		/*
		 * 
		 */		
		int subID = -1, objID = -1, preID = -1;
		{
			objID = Integer.parseInt(rdfUnit[2]);
			{
				if(objID >= subNum) return;
				reporter.incrCounter(oReport.edgeNum, 1);
			}
			subID = Integer.parseInt(rdfUnit[0]);
			preID = Integer.parseInt(rdfUnit[1]);
		}
		/*
		 * 
		 */
		{
			iSig = new Signature();
			offlineDriver.buildPreSignature(preID, iSig, 1);
			offlineDriver.buildEntitySignature(rdfUnit[5], iSig, offlineDriver.hashType, 1);
			// s_subID_level
			StringBuffer _sb = null;
			int _cur_subID = subID, _cur_objID = objID;
			int i_level = 0;
			while(true)
			{
				_sb = new StringBuffer("s_").append(_cur_subID).append("_").append(i_level);			
				output.collect(new Text(_sb.toString()), new mEntry(_cur_objID, iSig));
				{// for objID -> subID
					_sb = new StringBuffer("o_").append(_cur_objID).append("_").append(i_level);
					output.collect(new Text(_sb.toString()), new mEntry(_cur_subID, iSig));
				}
				_cur_subID = myHbase.getFather(_cur_subID, i_level);// father of root is -1
				_cur_objID = myHbase.getFather(_cur_objID, i_level);
				{
					// check more, when -1, i should be (levelNum - 1)
					{
						if((_cur_subID == -1 && _cur_objID > 0 ||
							_cur_subID > 0 && _cur_objID == -1) && _ii < 5)
						{
							_ii ++;
							reporter.setStatus("not same level: " + _cur_subID + "," + _cur_objID + "\t" +
									"\t" + subID + "," + objID);
							System.out.print("not same level: " + _cur_subID + "," + _cur_objID + "\t" +
									"\t" + subID + "," + objID);
						}
						if(_cur_objID == -1 && i_level != (levelNum - 1) && _ii < 5)
						{
							_ii ++;
							reporter.setStatus("not right level: " + i_level);
							System.out.print("not right level: " + i_level + ":" + levelNum + "\t" + subID + ":" + objID + "\n");
						}
					}
					if(_cur_subID == -1) break;
				}
				i_level ++;
			}
			_sb = null;
			iSig = null;
		}
	}
	private void forYago2_sumEdge(Text values, OutputCollector<Text, mEntry> output, Reporter reporter) throws IOException
	{
		{
			_ii ++;
//			if(_ii % 10000 != 0){
//				return;
//			}
		}
		String _line = values.toString();
//		String [] rdfUnit = _line.split("\t");
		{
			int _start = 0;
			int _count_tmp = 0;
			for(int i = 0; i < _line.length(); i ++)
			{
				if(_line.charAt(i) == '\t')
				{
					if(_count_tmp == 0){
						strSubID = _line.substring(_start, i);
					}
					else
					if(_count_tmp == 1){
						strPreID = _line.substring(_start, i);
					}
					else
					if(_count_tmp == 2){
						strObjID = _line.substring(_start, i);
					}
					else
					if(_count_tmp == 6){
						strOBJ = _line.substring(_start, i);
						break;
					}					
					_count_tmp ++;
					_start = i + 1;
				}
			}
		}
		/*
		 * 
		 */		
		int subID = -1, objID = -1, preID = -1, locID = -1;
		{
			subID = Integer.parseInt(strSubID);
			preID = Integer.parseInt(strPreID);
			objID = Integer.parseInt(strObjID);
//			locID = Integer.parseInt(rdfUnit[3]);
		}
		/*
		 * 
		 */
		{
			// s_subID_level
			int _cur_subID = subID, _cur_objID = objID;
			int i_level = 0;
			if(objID >= subNum)//with literal
			{
				iSig.clear();
				_sb.setLength(0);
				offlineDriver.buildPreSignature(preID, iSig, 1);
				_sb.append("l_").append(subID).append("_").append(i_level);
				tKey.set(_sb.toString());
				entityVal.setValue(_cur_subID, iSig);
				output.collect(tKey, entityVal);
				{
					reporter.incrCounter(oReport.ledgeNum, 1);
				}
				return;
			}
			iSig.clear();

			offlineDriver.buildPreSignature(preID, iSig, 1);
			offlineDriver.buildEntitySignature(strOBJ, iSig, offlineDriver.hashType, 1);
			{
				reporter.incrCounter(oReport.edgeNum, 1);
			}
			while(true)
			{
				_sb.setLength(0);
				_sb.append("s_").append(_cur_subID).append("_").append(i_level);	
				tKey.set(_sb.toString());
				entityVal.setValue(_cur_objID, iSig);
				output.collect(tKey, entityVal);
				{// for objID -> subID
					_sb.setLength(0);
					_sb.append("o_").append(_cur_objID).append("_").append(i_level);
					tKey.set(_sb.toString());
					entityVal.setValue(_cur_subID, iSig);
					output.collect(tKey, entityVal);
				}
				{//get subID father
					Integer _Isub = hmFatherCache[i_level].get(_cur_subID);
					if(_Isub == null)
					{
						int _pre_subID = _cur_subID;
						_cur_subID = myHbase.getFather(_cur_subID, i_level);// father of root is -1
						hmFatherCache[i_level].put(_pre_subID, _cur_subID);
					}
					else {
						_cur_subID = _Isub.intValue();
					}
				}
				{//get objID father
					Integer _Iobj = hmFatherCache[i_level].get(_cur_objID);
					if(_Iobj == null)
					{
						int _pre_objID = _cur_objID;
						_cur_objID = myHbase.getFather(_cur_objID, i_level);// father of root is -1
						hmFatherCache[i_level].put(_pre_objID, _cur_objID);
					}
					else {
						_cur_objID = _Iobj.intValue();
					}
				}				
				{
					if(_cur_subID == -1) break;
				}
				i_level ++;
				//special for yago2
				if(i_level >= offlineDriver.levelNum - 1) break;
			}
		}
	}
	private void forYago2(Text values, OutputCollector<Text, mEntry> output, Reporter reporter) throws IOException
	{
		String _line = values.toString();
		String [] rdfUnit = _line.split("\t");
		/*
		 * 
		 */		
		int subID = -1, objID = -1, preID = -1, locID = -1;
		{
			objID = Integer.parseInt(rdfUnit[2]);
			{
				if(objID >= subNum) return;
			}
			subID = Integer.parseInt(rdfUnit[0]);
			preID = Integer.parseInt(rdfUnit[1]);
			locID = Integer.parseInt(rdfUnit[3]);
		}
		/*
		 * 
		 */
		{
			iSig = new Signature();
			offlineDriver.buildLocSignature(rdfUnit[7], locID, iSig, 1);
			offlineDriver.buildPreSignature(preID, iSig, 1);
			offlineDriver.buildEntitySignature(rdfUnit[6], iSig, offlineDriver.hashType, 1);
			// s_subID_level
			StringBuffer _sb = null;
			int _cur_subID = subID, _cur_objID = objID;
			int i_level = 0;
			while(true)
			{
				_sb = new StringBuffer("s_").append(_cur_subID).append("_").append(i_level);			
				output.collect(new Text(_sb.toString()), new mEntry(_cur_objID, iSig));
				{// for objID -> subID
					_sb = new StringBuffer("o_").append(_cur_objID).append("_").append(i_level);
					output.collect(new Text(_sb.toString()), new mEntry(_cur_subID, iSig));
				}
				_cur_subID = myHbase.getFather(_cur_subID, i_level);// father of root is -1
				_cur_objID = myHbase.getFather(_cur_objID, i_level);
				{
					// check more, when -1, i should be (levelNum - 1)
					{
						if((_cur_subID == -1 && _cur_objID > 0 ||
							_cur_subID > 0 && _cur_objID == -1) && _ii < 5)
						{
							_ii ++;
							reporter.setStatus("not same level: " + _cur_subID + "," + _cur_objID + "\t" +
									"\t" + subID + "," + objID);
						}
						if(_cur_objID == -1 && i_level != levelNum && _ii < 5)
						{
							_ii ++;
							reporter.setStatus("not right level: " + i_level + "\t" + subID + ":" + objID + "\n");
						}
					}
					if(_cur_subID == -1) break;
				}
				i_level ++;
			}
			_sb = null;
			iSig = null;
		}
	}
	public void configure(JobConf _job)
	{
		subNum = (int) _job.getLong("subNum", -1);
		levelNum = (int)_job.getLong("levelNum", -1);
		if(hmFatherCache == null){
			hmFatherCache = new HashMap[10];
			int[] initSZ = new int[]{500*1000, 50*1000, 5*1000, 500};
			for(int i = 0; i < levelNum; i ++){
				hmFatherCache[i] = new HashMap<Integer, Integer>(initSZ[i]);
			}
		}
	}

}