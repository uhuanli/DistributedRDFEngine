package hbase;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import mapreduce.Signature;
import mapreduce.oReport;
import mapreduce.offlineDriver;

import org.apache.commons.lang.math.Fraction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.Reporter;
import org.jruby.runtime.callsite.SuperCallSite;

import buildCluster.Kmeans.mCenter;

import summaryGraph.mEntry;
import sun.security.krb5.internal.EncAPRepPart;

public class myHbase extends HBASE 
{
	private static Configuration conf;
	public static HBaseAdmin admin = null;
	private static HTable tSub2ID = null;
	private static HTable tPre2ID = null;
	private static HTable tObj2ID = null;
	private static HTable tLoc2ID = null;
	private static HTable tID2Sub = null;
	private static HTable tID2Pre = null;
	private static HTable tID2Obj = null;
	private static HTable tID2Loc = null;
	private static HTable tXY2LocID = null;
	private static HTable tLocID2XY = null;
	private static HTable tStree    = null;
	private static HTable tV2E      = null;
	private static HTable tE2E      = null;
	private static HTable tSumEdge      = null;
	/**
	 * 
	 */
	private static HashMap<String, Integer> hmSub2ID = null;
	private static HashMap<String, Integer> hmPre2ID = null;
	private static HashMap<String, Integer> hmObj2ID = null;
	private static HashMap<String, Integer> hmLoc2ID = null;
	
	private static HashMap<Integer, String>[] hmVSigCache = null;
	private static int[] hmVSigSize = new int[]{1000*1000, 100*1000, 10*1000, 1*1000, 100};
	private static ArrayList<String> subList = null;
	private static ArrayList<String> objList = null;
	private static ArrayList<String> locList = null;
	
	static{
		conf = myHbase.myConfigure();
	}
	
	public static void closeTable(HTable _table) throws IOException{
		if(_table != null)	_table.close();
	}
	public static void closeAll() throws IOException{
		myHbase.closeTable(tE2E);tE2E = null;
		myHbase.closeTable(tID2Loc);tID2Loc = null;
		myHbase.closeTable(tID2Obj);tID2Obj = null;
		myHbase.closeTable(tID2Pre);tID2Pre = null;
		myHbase.closeTable(tID2Sub);tID2Sub = null;
		myHbase.closeTable(tLoc2ID);tLoc2ID = null;
		myHbase.closeTable(tLocID2XY);tLocID2XY = null;
		myHbase.closeTable(tObj2ID);tObj2ID = null;
		myHbase.closeTable(tPre2ID);tPre2ID = null;
		myHbase.closeTable(tSub2ID);tSub2ID = null;
		myHbase.closeTable(tStree);tStree = null;
		myHbase.closeTable(tSumEdge);tSumEdge = null;
		myHbase.closeTable(tV2E);tV2E = null;
		myHbase.closeTable(tXY2LocID);tXY2LocID = null;
		admin.close();
		admin = null;
	}
	
	public static boolean existEdge(int _sID, int _oID, int _level) throws IOException
	{
		myHbase.E2Echeck();
		String _e = offlineDriver.encodeEdge(_sID, _oID);
		return HBASE.existRow(tE2E, _e);
	}
	public static Signature getESignature(int _sID, int _oID, int _level) throws IOException
	{//three huge bugs!!!
		myHbase.E2Echeck();
		String _e = offlineDriver.encodeEdge(_sID, _oID);
		String _levelF = offlineDriver.levelFamily[_level];
		byte[] vals = HBASE.getValue(tE2E, _e, _levelF, offlineDriver.eSignature);
		if(vals == null) return null;
		return new Signature(new String(vals));
	}
	public static ArrayList<mEntry> getSonList(int _subID, int _level) throws IOException
	{
		ArrayList<mEntry> _ret = new ArrayList<mEntry>();
		myHbase.sTreeCheck();
		Result _r = HBASE.getRow(tStree, String.valueOf(_subID));
		String _family = offlineDriver.levelFamily[_level];
		int sz = Integer.parseInt(new String(_r.getValue(_family.getBytes(), offlineDriver.size.getBytes())));
		for(int i = 0; i < sz; i ++)
		{
			_ret.add(new mEntry(new String( _r.getValue(_family.getBytes(), (i + "").getBytes()) )));
		}
		return _ret;
	}
	public static int getFather(int _subID, int _cur_level) throws IOException
	{
		String _family = offlineDriver.levelFamily[_cur_level];
		String _qualifier = offlineDriver.father;
		{
			myHbase.sTreeCheck();
			byte[] _vals = HBASE.getValue(tStree, String.valueOf(_subID), _family, _qualifier);
			if(_vals == null) 
				return -1;
			return Integer.parseInt(new String(_vals));
		}
	}
	private static void sTreeCheck()
	{
		if(myHbase.tStree == null)
		{
			try {
				if(conf == null){
					System.err.print("conf is null!!");
				}
				myHbase.tStree = new HTable(conf, offlineDriver.sTree);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.print("sTree is null!!\n");
			}
		}
	}
	public static void createSumEdgeTable() throws IOException{
		myHbase.createTable(offlineDriver.sumEdge, offlineDriver.levelFamily);
	}
	private static boolean SumEdgecheck() {
		if(tSumEdge == null)
		{
			try {
				tSumEdge = new HTable(conf, offlineDriver.sumEdge);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}
	public static String getVSignature(int _id, int _level) throws IOException
	{
		myHbase.sTreeCheck();
		{
			if(hmVSigCache == null) hmVSigCache = new HashMap[10];
			if(hmVSigCache[_level] == null)
			{
				hmVSigCache[_level] = new HashMap<Integer, String>(hmVSigSize[_level]);
			}
			String _v_sig = hmVSigCache[_level].get(_id);
			if(_v_sig != null) return _v_sig;
		}
		String _row = _id + "";
		String _levelF = offlineDriver.levelFamily[_level];
		String _qualifier = offlineDriver.signature;
		byte[] _val = null;
		{
			_val = HBASE.getValue(tStree, _row, _levelF, _qualifier);
			if(_val == null) 
			{
				System.err.print("vSig bug\n");
				return null;
			}
		}
		String _sig_string = new String(_val);
		hmVSigCache[_level].put(_id, _sig_string);
		return _sig_string;
	}
	public static void loadSumEdgeValue(String _row, String _family, String _qualifier, String _val) throws IOException{
		myHbase.SumEdgecheck();
		HBASE.addRow(tSumEdge, _row, _family, _qualifier, _val);	
	}
	public static void loadLiteralEdge(int _id, int _level, ArrayList<mEntry> _e_list) throws IOException
	{
		myHbase.SumEdgecheck();
		
		String _row = _id + "";
		String _val = null;
		String _levelF = offlineDriver.levelFamily[_level];
		{
			_val = _e_list.size() + "";
			HBASE.addRow(tSumEdge, _row, _levelF, offlineDriver.LiteralSize, _val);
		}
		StringBuffer _sb = new StringBuffer();
		for(int i = 0; i < _e_list.size(); i ++)
		{
			mEntry _m = _e_list.get(i);
			{
				_sb.setLength(0);
				//objID eSig
				_sb.append(_m.getId()).append("\t").append(_m.getSig().toString());
				_val = _sb.toString();
			}
			String _qualifier = "l" + i;
			HBASE.addRow(tSumEdge, _row, _levelF, _qualifier, _val);
		}
	}
	public static void loadInEdges(int _id, int _level, ArrayList<mEntry> _e_list) throws IOException
	{
		myHbase.SumEdgecheck();
		
		String _row = _id + "";
		String _val = null;
		String _levelF = offlineDriver.levelFamily[_level];
		{
			_val = _e_list.size() + "";
			HBASE.addRow(tSumEdge, _row, _levelF, offlineDriver.InSize, _val);
		}
		for(int i = 0; i < _e_list.size(); i ++)
		{
			mEntry _m = _e_list.get(i);
			{
				StringBuffer _sb = new StringBuffer();
				String _to_sig = myHbase.getVSignature(_m.getId(), _level);
				//toID toSig eSig
				_sb.append(_m.getId()).append("\t").append(_to_sig).append("\t").append(_m.getSig().toString());
				_val = _sb.toString();
			}
			String _qualifier = "in" + i;
			HBASE.addRow(tSumEdge, _row, _levelF, _qualifier, _val);
		}
	}
	public static void loadOutEdges(int _id, int _level, ArrayList<mEntry> _e_list) throws IOException
	{
		myHbase.SumEdgecheck();
		
		String _row = _id + "";
		String _val = null;
		String _levelF = offlineDriver.levelFamily[_level];
		{
			_val = _e_list.size() + "";
			HBASE.addRow(tSumEdge, _row, _levelF, offlineDriver.OutSize, _val);
		}
		for(int i = 0; i < _e_list.size(); i ++)
		{
			mEntry _m = _e_list.get(i);
			{
				StringBuffer _sb = new StringBuffer();
				String _from_sig = myHbase.getVSignature(_m.getId(), _level);
				//toID toSig eSig
				_sb.append(_m.getId()).append("\t").append(_from_sig).append("\t").append(_m.getSig().toString());
				_val = _sb.toString();
			}
			String _qualifier = "out" + i;
			HBASE.addRow(tSumEdge, _row, _levelF, _qualifier, _val);
		}
	}
	private static void V2Echeck()
	{
		if(tV2E == null)
		{
			try {
				tV2E = new HTable(conf, offlineDriver.V2E);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private static void E2Echeck()
	{
		if(tE2E == null)
		{
			try {
				tE2E = new HTable(conf, offlineDriver.E2E);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static void VFirstInE(int _subID, int _inID, int _level) throws IOException
	{
		String _e = offlineDriver.encodeEdge(_inID, _subID);
		String _levelF = offlineDriver.levelFamily[_level];
		String _qualifier = offlineDriver.firstInE;
		myHbase.V2Echeck();
		HBASE.addRow(tV2E, String.valueOf(_subID), _levelF, _qualifier, _e);
	}
	public static void VFirstOutE(int _subID, int _outID, int _level) throws IOException
	{
		String _e = offlineDriver.encodeEdge(_subID, _outID);
		String _levelF = offlineDriver.levelFamily[_level];
		String _qualifier = offlineDriver.firstOutE;
		myHbase.V2Echeck();
		HBASE.addRow(tV2E, String.valueOf(_subID), _levelF, _qualifier, _e);
	}
	public static void ENextInE(String _E, String _outE, int _level) throws IOException
	{
		myHbase.E2Echeck();
		String _levelF = offlineDriver.levelFamily[_level];
		String _qualifier = offlineDriver.nextInE;
		HBASE.addRow(tE2E, _E, _levelF, _qualifier, _outE);
	}
	public static void ENextOutE(String _E, String _outE, int _level) throws IOException
	{
		myHbase.E2Echeck();		
		String _levelF = offlineDriver.levelFamily[_level];
		String _qualifier = offlineDriver.nextOutE;
		HBASE.addRow(tE2E, _E, _levelF, _qualifier, _outE);
	}
	public static void setEsignature(String _E, Signature _sig, int _level) throws IOException
	{
		myHbase.E2Echeck();
		String _levelF = offlineDriver.levelFamily[_level];
		String _qualifier = offlineDriver.eSignature;
		String _sig_str = _sig.toString();
		HBASE.addRow(tE2E, _E, _levelF, _qualifier, _sig_str);
	}
	public static void setEsigList(int _subID, ArrayList<mEntry> _e_list, int _level) throws IOException
	{
		for(mEntry iEntry : _e_list)
		{
			String _e = offlineDriver.encodeEdge(_subID, iEntry.getId());
			setEsignature(_e, iEntry.getSig(), _level);
		}
	}
	public static void createV2ETable() throws IOException
	{
		myHbase.createTable(offlineDriver.V2E, offlineDriver.levelFamily);
	}
	public static void createV2VTable() throws IOException
	{
		myHbase.createTable(offlineDriver.E2E, offlineDriver.levelFamily);
	}
	//_m is a new center with new signature(formed by OR operations on list)
	public static void submitCluster(mCenter _m, ArrayList<mCenter> cList, int _b, int _e,
			int _level) throws IOException
	{
		myHbase.sTreeCheck();
		int _num_son = _e - _b;
		String next_family = offlineDriver.sTreeFamily[_level + 1];// for next level
		String cur_family = offlineDriver.sTreeFamily[_level];
		String _c_sub = _m.getSub();
		String _sig = _m.getSig().toString();
		/*
		 * sig, size, 1, 2, 3, 4(sonlist)
		 */
		HBASE.addRow(tStree, _c_sub, next_family, offlineDriver.signature, _sig);
		HBASE.addRow(tStree, _c_sub, next_family, offlineDriver.size, _num_son + "");
		for(int i = _b; i < _e; i ++)
		{
			mCenter _ctmp = cList.get(i);
			String _value = _ctmp.toString();
			// add son
			HBASE.addRow(tStree, _c_sub, next_family, offlineDriver.sons[i - _b], _value);
			// set father to son[i]
			String sonSub = _ctmp.getSub();
			HBASE.addRow(tStree, sonSub, cur_family, offlineDriver.father, _c_sub);
			{
				if(_level == 0) 
					HBASE.addRow(tStree, sonSub, cur_family, offlineDriver.signature, _ctmp.getSig().toString());
			}
		}
	}
	public static void createStreeTable() throws IOException
	{
		myHbase.createTable(offlineDriver.sTree, offlineDriver.sTreeFamily);
	}
	public static void createIDtable() throws IOException
	{
		{
			myHbase.clearIDtable();
		}
		myHbase.createTable(offlineDriver.sub2ID, new String[]{"ID"});
		myHbase.createTable(offlineDriver.pre2ID, new String[]{"ID"});
		myHbase.createTable(offlineDriver.obj2ID, new String[]{"ID"});
		myHbase.createTable(offlineDriver.loc2ID, new String[]{"ID"});
		myHbase.createTable(offlineDriver.ID2sub, new String[]{"Str"});
		myHbase.createTable(offlineDriver.ID2pre, new String[]{"Str"});
		myHbase.createTable(offlineDriver.ID2obj, new String[]{"Str"});
		myHbase.createTable(offlineDriver.ID2loc, new String[]{"Str"});
		myHbase.createTable(offlineDriver.XY2locID, new String[]{"ID"});
		myHbase.createTable(offlineDriver.locID2XY, new String[]{"XY"});
	}
	private static void clearIDtable() throws IOException {
		HBASE.deleteTable(admin, offlineDriver.sub2ID);
		HBASE.deleteTable(admin, offlineDriver.pre2ID);
		HBASE.deleteTable(admin, offlineDriver.obj2ID);
		HBASE.deleteTable(admin, offlineDriver.loc2ID);
		HBASE.deleteTable(admin, offlineDriver.ID2sub);
		HBASE.deleteTable(admin, offlineDriver.ID2pre);
		HBASE.deleteTable(admin, offlineDriver.ID2obj);
		HBASE.deleteTable(admin, offlineDriver.ID2loc);
		HBASE.deleteTable(admin, offlineDriver.XY2locID);
		HBASE.deleteTable(admin, offlineDriver.locID2XY);
		
	}
	private static String encodesub(double X, double Y)
	{
		StringBuffer _sb = new StringBuffer();
		_sb.append(X).append("/").append(Y);
		return _sb.toString();
	}
	public static int getLocIDByXY(double X, double Y)
	{
		String _xy = encodesub(X, Y);
		return getLocIDByXY(_xy);
	}
	public static int getLocIDByXY(String _xy)
	{
		{
			if(tXY2LocID == null)
				try {
					tXY2LocID = new HTable(conf, offlineDriver.XY2locID);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return getIDByStr(tXY2LocID, _xy);
	}
	public static String getXYByLocID(int locID)
	{
		{
			if(tLocID2XY == null)
				try {
					tLocID2XY = new HTable(conf, offlineDriver.locID2XY);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		byte[] vals = null;
		try {
			vals = myHbase.getValue(tLocID2XY, locID + "", "XY", "xy");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(vals == null) return null;
		return Bytes.toString(vals);
	}
	public static void setLocID2XY(int locID, String _xy)
	{
		{
			if(tLocID2XY == null)
				try {
					tLocID2XY = new HTable(conf, offlineDriver.locID2XY);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		try {
			HBASE.addRow(tLocID2XY, locID + "", "XY", "xy", _xy);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void setXY2LocID(String _sub, int locID)
	{
		{
			if(tXY2LocID == null)
				try {
					tXY2LocID = new HTable(conf, offlineDriver.XY2locID);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		try {
			HBASE.addRow(tXY2LocID, _sub, "ID", "id", locID + "");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void setSub2ID(String _sub, int _id) throws IOException
	{
		{
			if(tSub2ID == null)
			{
				tSub2ID = new HTable(conf, offlineDriver.sub2ID);
			}
		}
		myHbase.setStr2ID(tSub2ID, _sub, _id);
	}
	public static void setID2Sub(int _id, String _sub) throws IOException
	{
		{
			if(tID2Sub == null)
			{
				tID2Sub = new HTable(conf, offlineDriver.ID2sub);
			}
		}
		myHbase.setID2Str(tID2Sub, _id, _sub);
	}
	public static void setPre2ID(String _pre, int _id) throws IOException
	{
		{
			if(tPre2ID == null)
			{
				tPre2ID = new HTable(conf, offlineDriver.pre2ID);
			}
		}
		myHbase.setStr2ID(tPre2ID, _pre, _id);
	}
	public static void setID2Pre(int _id, String _pre) throws IOException
	{
		{
			if(tID2Pre == null)
			{
				tID2Pre = new HTable(conf, offlineDriver.ID2pre);
			}
		}
		myHbase.setID2Str(tID2Pre, _id, _pre);
	}
	public static void setObj2ID(String _obj, int _id) throws IOException
	{
		{
			if(tObj2ID == null)
			{
				tObj2ID = new HTable(conf, offlineDriver.obj2ID);
			}
		}
		myHbase.setStr2ID(tObj2ID, _obj, _id);
	}
	public static void setID2Obj(int _id, String _obj) throws IOException
	{
		{
			if(tID2Obj == null)
			{
				tID2Obj = new HTable(conf, offlineDriver.ID2obj);
			}
		}
		myHbase.setID2Str(tID2Obj, _id, _obj);
	}
	public static void setLoc2ID(String _loc, int _id) throws IOException
	{
		{
			if(tLoc2ID == null)
			{
				tLoc2ID = new HTable(conf, offlineDriver.loc2ID);
			}
		}
		myHbase.setStr2ID(tLoc2ID, _loc, _id);
	}
	public static void setID2Loc(int _id, String _loc) throws IOException
	{
		{
			if(tID2Loc == null)
			{
				tID2Loc = new HTable(conf, offlineDriver.ID2loc);
			}
		}
		myHbase.setID2Str(tID2Loc, _id, _loc);
	}
	public static int getSubIDfromHbase(String _str)
	{
		{
			if(tSub2ID == null)
				try {
					tSub2ID = new HTable(conf, Bytes.toBytes(offlineDriver.sub2ID));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
		}
		return getIDByStr(tSub2ID, _str);
	}
	private static int getIDByStr(HTable _table, String _str)
	{
		byte[] vals = null;
		try {
			vals = HBASE.getValue(_table, _str, "ID", "id");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		{
			if(vals == null)
			{
				return -1;
			}
		}
		return Integer.parseInt(new String(vals));
	}
	private static void setStr2ID(HTable _table, String _str, int _id) throws IOException
	{
		HBASE.addRow(_table, _str, "ID", "id", _id + "");
	}
	private static void setID2Str(HTable _table, int _id, String _str) throws IOException
	{
		HBASE.addRow(_table, _id + "", "Str", "str", _str);
	}
	public static int getObjIDfromHbase(String _str)
	{
		{
			if(tObj2ID == null)
				try {
					tObj2ID = new HTable(conf, Bytes.toBytes(offlineDriver.obj2ID));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
		return getIDByStr(tObj2ID, _str);
	}
	public static int getLocIDfromHbase(String _str)
	{
		{
			if(tLoc2ID == null)
				try {
					tLoc2ID = new HTable(conf, Bytes.toBytes(offlineDriver.loc2ID));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return getIDByStr(tLoc2ID, _str);
	}
	public static void loadPreID() throws IOException
	{
		if(hmPre2ID != null) return;
		
		{
			if(tPre2ID == null)
				try {
					tPre2ID = new HTable(conf, Bytes.toBytes(offlineDriver.pre2ID));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
		}
		hmPre2ID = new HashMap<String, Integer>(200);
		ResultScanner _rs = tPre2ID.getScanner(Bytes.toBytes("ID"));
		for(Result iR : _rs)
		{
			String _pre = Bytes.toString(iR.getRow());
			String _pre_id = Bytes.toString(iR.getValue(Bytes.toBytes("ID"), Bytes.toBytes("id")));
			hmPre2ID.put(_pre, Integer.parseInt(_pre_id));
		}
	}
	/*
	 * we will look up sub table before get an objID
	 */
	public static void loadSubID() throws IOException
	{
		if(hmSub2ID != null) return;
		
		{
			if(tSub2ID == null)
				try {
					tSub2ID = new HTable(conf, Bytes.toBytes(offlineDriver.sub2ID));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
		}
		
		hmSub2ID = new HashMap<String, Integer>(offlineDriver.SubScale);
		ResultScanner _rs = tSub2ID.getScanner(Bytes.toBytes("ID"));
		for(Result iR : _rs)
		{
			String _sub = Bytes.toString(iR.getRow());
			String _sub_id = Bytes.toString(iR.getValue(Bytes.toBytes("ID"), Bytes.toBytes("id")));
			hmSub2ID.put(_sub, Integer.parseInt(_sub_id));
		}
	}
	public static int getIDBySub(String _str)
	{
		{
			if(hmSub2ID == null)
			{
				try {
					myHbase.loadSubID();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		Integer _i = hmSub2ID.get(_str);
		if(_i == null) return -1;
		return _i.intValue();
	}
	/*
	 * you can just build cache, which is the same
	 */
	public static int getIDByPre(String _str)
	{
		{
			if(hmPre2ID == null)
			{
				try {
					myHbase.loadPreID();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		Integer _i = hmPre2ID.get(_str);
		return _i.intValue();
	}
	public static int getIDByObj(String _str)
	{
		{
			/**
			 * look sub2ID first!!
			 */
			int objID = getIDBySub(_str);
			if(objID != -1)	return objID;
		}
		return getObjIDfromHbase(_str);
	}
	public static int getIDByLoc(String _str)
	{
		return getLocIDfromHbase(_str);
	}
	public static void initial()
	{
		try {
			bwLog = new BufferedWriter(new FileWriter("/root/log"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void close() throws IOException
	{
		if(tSub2ID != null) tSub2ID.close();
		if(tPre2ID != null) tPre2ID.close();
		if(tObj2ID != null) tObj2ID.close();
		if(tLoc2ID != null) tLoc2ID.close();
		if(tID2Sub != null) tID2Sub.close();
		if(tID2Pre != null) tID2Pre.close();
		if(tID2Obj != null) tID2Obj.close();
		if(tID2Loc != null) tID2Loc.close();
		if(tXY2LocID != null) tXY2LocID.close();
		if(tLocID2XY != null) tLocID2XY.close();
		if(tStree != null) tStree.close();
		bwLog.close();
	}
	public static void createTable(String _table_name, String _cfs[]) throws IOException
	{
		{
			if(admin == null){
				admin = new HBaseAdmin(conf);
			}
			if(admin.tableExists(_table_name))
			{
				admin.disableTable(_table_name);
				admin.deleteTable(_table_name);
			}
		}
		HBASE.createTable(admin, _table_name, _cfs);
	}
	private static Configuration myConfigure() 
	{
		return HBASE.Configure(offlineDriver.ZK);
	}
	public static Configuration getConf()
	{
		return conf;
	}
	public static void checkID() throws IOException
	{
		System.out.print("begin...\n");
		int _c_s2id = myHbase.checkIDtable(tID2Sub, tSub2ID);
		{
//			reporter.incrCounter(oReport.cS2ID, _c_s2id);
			System.out.print("Done s2id\n");
			bwLog.write("s2id done\n");
		}
		int _c_p2id = myHbase.checkIDtable(tID2Pre, tPre2ID);
		{
//			reporter.incrCounter(oReport.cP2ID, _c_p2id);
			System.out.print("Done p2id\n");
			bwLog.write("p2id done\n");
		}
		int _c_o2id = myHbase.checkIDtable(tID2Obj, tObj2ID);
		{
//			reporter.incrCounter(oReport.cO2ID, _c_o2id);
			System.out.print("Done o2id\n");
			bwLog.write("o2id done\n");
		}
		int _c_l2id = myHbase.checkIDtable(tID2Loc, tLoc2ID);
		{
//			reporter.incrCounter(oReport.cL2ID, _c_l2id);
			System.out.print("Done l2id\n");
			bwLog.write("l2id done\n");
		}
	}
	public static void initalTable() throws IOException
	{
		if(tSub2ID == null)	tSub2ID = new HTable(conf, offlineDriver.sub2ID);
		if(tPre2ID == null)	tPre2ID = new HTable(conf, offlineDriver.pre2ID);
		if(tObj2ID == null)	tObj2ID = new HTable(conf, offlineDriver.obj2ID);
		if(tLoc2ID == null)	tLoc2ID = new HTable(conf, offlineDriver.loc2ID);
		
		if(tID2Sub == null) tID2Sub = new HTable(conf, offlineDriver.ID2sub);
		if(tID2Pre == null) tID2Pre = new HTable(conf, offlineDriver.ID2pre);
		if(tID2Obj == null) tID2Obj = new HTable(conf, offlineDriver.ID2obj);
		if(tID2Loc == null) tID2Loc = new HTable(conf, offlineDriver.ID2loc);
	}
	public static BufferedWriter bwLog = null;
	public static int checkIDtable(HTable _tableID2Str, HTable _tableStr2ID) throws IOException
	{
		ResultScanner _rs = _tableID2Str.getScanner(Bytes.toBytes("Str"));
		int _count = 0;
		bwLog.write(new String(_tableID2Str.getTableName()));
		for(Result iR : _rs)
		{
			String _row = Bytes.toString(iR.getRow());
			String _valStr = Bytes.toString(iR.getValue(Bytes.toBytes("Str"), Bytes.toBytes("str")));
			String _vRow = String.valueOf( myHbase.getIDByStr(_tableStr2ID, _valStr) );
			if(! _row.equals(_vRow))
			{
				_count ++;
				System.out.print(_row + "\t" + _valStr + "\t" + _vRow + "\n");
				bwLog.write(_row + "\t" + _valStr + "\t" + _vRow + "\n");
			}
			_count ++;
			{
				if(_count % 10000 == 0)
				{
					System.out.print("\t=>" + _count);
					bwLog.write("\t=>" + _count);
				}
				if(_count % 100000 == 0)
				{
					System.out.print("\n");
					bwLog.write("\n");
				}
			}
		}
		return _count;
	}
	public static void ScanGenerateID() throws IOException
	{
		String prePath = "/root/generateID/";
		
		ScanTable(tSub2ID, "ID", "id", prePath + "sub2ID", offlineDriver.sub2ID);
		System.out.print("Done sub2ID\n");
		ScanTable(tID2Sub, "Str", "str", prePath + "ID2sub", offlineDriver.ID2sub);
		System.out.print("Done ID2sub\n");
		
		ScanTable(tPre2ID, "ID", "id", prePath + "pre2ID", offlineDriver.pre2ID);
		System.out.print("Done pre2ID\n");
		ScanTable(tID2Pre, "Str", "str", prePath + "ID2pre", offlineDriver.ID2pre);
		System.out.print("Done ID2pre\n");
		
//		ScanTable(tObj2ID, "ID", "id", prePath + "obj2ID", offlineDriver.obj2ID);
//		System.out.print("Done obj2ID\n");
//		ScanTable(tID2Obj, "Str", "str", prePath + "ID2obj", offlineDriver.ID2obj);
//		System.out.print("Done ID2obj\n");
		
		ScanTable(tLoc2ID, "ID", "id", prePath + "Loc2ID", offlineDriver.loc2ID);
		System.out.print("Done loc2ID\n");
		ScanTable(tID2Loc, "Str", "str", prePath + "ID2Loc", offlineDriver.ID2loc);
		System.out.print("Done ID2Loc\n");
		/*
		 * 
		 */
		
		
		
	}
	public static void ScanTable(HTable _table, String _family, String _qualifier, String _path, String _table_name) throws IOException
	{
		{
			if(_table == null)	_table = new HTable(conf, _table_name);
		}
		BufferedWriter _bw = new BufferedWriter(new FileWriter(_path));
		ResultScanner _rs = _table.getScanner(Bytes.toBytes(_family));
		int _count = 0;
		for(Result iR : _rs)
		{
			String _row = Bytes.toString(iR.getRow());
			String _val = Bytes.toString(iR.getValue(Bytes.toBytes(_family), Bytes.toBytes(_qualifier)));
			_bw.write(_row + "\t" + _val + "\n");
			{
				_count ++;
				if(_count % 10000 == 0) 
					System.out.print("\t=>" + _count);
				if(_count % 100000 == 0)
					System.out.print("\n");
			}
		}
		_bw.close();
	}
	public static void scanFather() throws IOException
	{
		myHbase.sTreeCheck();
		ResultScanner _rs = tStree.getScanner("level0".getBytes());
		for(Result iR : _rs)
		{
			if(iR.getValue("level0".getBytes(), offlineDriver.father.getBytes()) == null)
			{
				System.out.print(new String(iR.getRow()) + "\n");
			}
		}
	}
}
