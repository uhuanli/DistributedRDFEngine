package hbase;

import java.io.IOException;
import java.util.ArrayList;

import mapreduce.Signature;
import mapreduce.offlineDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.thrift.generated.Hbase.isTableEnabled_args;
import org.apache.hadoop.mapred.JobConf;

import summaryGraph.mEntry;

public class hbaseTest 
{
	public static void main(String[] args) throws IOException
	{
		myHbase.initial();
		{
			checkSig();
		}
		myHbase.close();
	}
	public static void checkSig() throws IOException
	{
//		int _id = 2584268;
		int _level = 0;
//		byte[] _s = myHbase.getValue(new HTable(myHbase.getConf(), "sTree"), "1710771", "level0", "sg");
		byte[] _s = myHbase.getValue(new HTable(myHbase.getConf(), "sTree"), "3636507", "level2", "sg");
//		byte[] _s = myHbase.getValue(new HTable(myHbase.getConf(), "sTree"), "1492802", "level1", "sg");
		Signature _sig = new Signature(new String(_s));
		System.out.println(_sig.toString() + "\n");
		System.out.print(_sig.bitToString());
	}
	public static void checkFather()
	{
		try {
			myHbase.scanFather();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static boolean checkSummary() throws IOException
	{
		int _sID = 1000046;
		int _oID = 1088324;
		ArrayList<mEntry> sList = myHbase.getSonList(_sID, 1);
		ArrayList<mEntry> oList = myHbase.getSonList(_oID, 1);
		Signature orSig = new Signature();
		{
//			System.out.print(sList.size() + "-\n" + oList.size() + "-\n");
//			for(mEntry sEntry : sList) sEntry.Print();
//			System.out.print("++++\n");
//			for(mEntry oEntry : oList) oEntry.Print();
		}
		for(mEntry sEntry : sList)
		{
			for(mEntry oEntry : oList)
			{
				int _s_id = sEntry.getId();
				int _o_id = oEntry.getId();
				Signature _stmp = myHbase.getESignature(_s_id, _o_id, 0);
				if(_stmp != null)
				{
					orSig.or(_stmp);
				}
			}
		}
		System.out.print(orSig.toString() + "\n");
		return false;
	}
	public static void checkCluster(String _subID) throws IOException
	{
		HTable _stree = new HTable(myHbase.getConf(), offlineDriver.sTree);
		String _sub_id = "1492802";
		Result _r = HBASE.getRow(_stree, _sub_id);
		String levelF = offlineDriver.levelFamily[1];
		String szStr = offlineDriver.size;
		String sigQ = offlineDriver.signature;
		String _sigStr = new String(_r.getValue(levelF.getBytes(), sigQ.getBytes()));
		Signature iSig = new Signature(_sigStr);
		int sz = Integer.parseInt(new String(_r.getValue(levelF.getBytes(), szStr.getBytes())));
		Signature orSig = new Signature();
		{
			System.out.print(sz + "\n");
		}
		for(int i = 0; i < sz; i ++)
		{
			String _sig_str = new String(_r.getValue(levelF.getBytes(), ("" + i).getBytes()));
			orSig.or(new Signature(_sig_str.split("\t")[1]));
//			if(_sig_str.split("\t")[0].equals("1989860"))
//			System.out.print(_sig_str.split("\t")[0] + "\n");
		}
		if(!(iSig.sigEqual(orSig)))
		{
			System.out.print(iSig.bitToString() + "\n");
			System.out.print(orSig.bitToString() + "\n");
		}
		else {
			System.out.print("fine!\n");
			System.out.print(iSig.bitToString() + "\n");
		}
	}
	public static void checkPath() throws IOException
	{
//		FileSystem fs = FileSystem.get(new JobConf());
//		for(int i = 0; i < offlineDriver.centerFileNum; i ++)
//		{
//			String _part = "part-000";
//			if(i < 10) 
//			{
//				_part += "0" + i;
//			}
//			else
//			{
//				_part += i;
//			}
//			Path _path = new Path(offlineDriver.centerPath + _part);
//			if(! fs.exists(_path))
//			{
//				System.out.print(i + " ");
//			}
//		}
	}
}


