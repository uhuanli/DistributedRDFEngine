package mapreduce;
import generateID.Table2FileMapper;
import generateID.Table2FileReducer;
import generateID.deleteDupMapper;
import generateID.deleteDupReducer;
import generateID.f_generateIDMapper;
import generateID.f_generateIDReducer;
import generateID.generateIDMapper;
import generateID.generateIDReducer;
import generateID.loadIDMapper;
import generateID.loadIDReducer;
import generateID.seq2TextMapper;
import generateID.set2TextReducer;
import hbase.myHbase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.math.Fraction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.thrift.generated.Hbase.createTable_args;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
//import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import summaryGraph.edgeIDMapper;
import summaryGraph.edgeIDReducer;
import summaryGraph.loadSumEdgeMapper;
import summaryGraph.loadSumEdgeReducer;
import summaryGraph.mEntry;
import summaryGraph.smGraphMapper;
import summaryGraph.smGraphReducer;
import summaryGraph.summaryGraphMapper;
import summaryGraph.summaryGraphReducer;

import addID2yago.addID2yagoMapper;
import addID2yago.addID2yagoReducer;
import buildCluster.loadLevelMapper;
import buildCluster.loadLevelReducer;
import buildCluster.treeLevelMapper;
import buildCluster.treeLevelReducer;
import buildCluster.Kmeans.KmeansMapper;
import buildCluster.Kmeans.KmeansReducer;
import buildCluster.Kmeans.findCenterMapper;
import buildCluster.Kmeans.findCenterReducer;
import buildSeqFile.buildSeqFileMapper;
import buildSeqFile.buildSeqFileReducer;
import buildSignature.buildSigMapper;
import buildSignature.buildSigReducer;

public class offlineDriver {

	public static String ZK = 
		"slave1,slave2,slave3,slave4,slave6,slave7,slave8";
	public static String sub2ID = "sub2ID";
	public static String pre2ID = "pre2ID";
	public static String obj2ID = "obj2ID";
	public static String loc2ID = "loc2ID";
	public static String ID2sub = "ID2sub";
	public static String ID2pre = "ID2pre";
	public static String ID2obj = "ID2obj";
	public static String ID2loc = "ID2loc";
	public static String XY2locID = "XY2locID";
	public static String locID2XY = "locID2XY";
	public static String V2E      = "V2E";
	public static String E2E      = "E2E";
	public static String sTree = "sTree";
	public static String sumEdge = "sumEdge";
	public static String InSize = "insz";
	public static String OutSize = "outsz";
	public static String LiteralSize = "lsz";
	/*
	 * qualifier: sig & size & sonList:1, 2, 3, 4...
	 */
	public final static String[] sTreeFamily = new String[]{
		"level0", "level1", "level2", "level3", "level4",
		"level5", "level6", "level7", "level8", "level9"
	};
	// level0 : signatre, father. leveli : signature, father(root's is -1), size, 1, 2, ...  
	public final static String[] levelFamily = new String[]{
		"level0", "level1", "level2", "level3", "level4", 
		"level5", "level6", "level7", "level8", "level9", 
	};
	public static String nextInE = "nie";
	public static String nextOutE = "noe";
	public static String firstInE = "fie";
	public static String firstOutE = "foe";
	public static String eSignature = "eSig";
	public final static String signature = "sg";
	public final static String father = "ft";
	public final static String size = "sz";
	public final static String[] sons = new String[]{
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", 
		"11", "12", "13", "14", "15", "16", "17", "18", "19", "20", 
		"21", "22", "23", "24", "25", "26", "27", "28", "29", "30", 
		"31", "32", "33", "34", "35", "36", "37", "38", "39", "40", 
		"41", "42", "43", "44", "45", "46", "47", "48", "49", "50", 
		"51", "52", "53", "54", "55", "56", "57", "58", "59", "60", 
		"61", "62", "63", "64", "65", "66", "67", "68", "69", "70", 
		"71", "72", "73", "74", "75", "76", "77", "78", "79", "80", 
		"81", "82", "83", "84", "85", "86", "87", "88", "89", "90", 
		"91", "92", "93", "94", "95", "96", "97", "98", "99", "100", 
		"101", "102", "103", "104", "105", "106", "107", "108", "109", "110", 
		"111", "112", "113", "114", "115", "116", "117", "118", "119", "120", 
		"121", "122", "123", "124", "125", "126", "127", "128", "129", "130", 
		"131", "132", "133", "134", "135", "136", "137", "138", "139", "140", 
		"141", "142", "143", "144", "145", "146", "147", "148", "149", "150", 
		"151", "152", "153", "154", "155", "156", "157", "158", "159", "160", 
		"161", "162", "163", "164", "165", "166", "167", "168", "169", "170", 
		"171", "172", "173", "174", "175", "176", "177", "178", "179", "180", 
		"181", "182", "183", "184", "185", "186", "187", "188", "189", "190", 
		"191", "192", "193", "194", "195", "196", "197", "198", "199", "200", 
		"201", "202", "203", "204", "205", "206", "207", "208", "209", "210", 
		"211", "212", "213", "214", "215", "216", "217", "218", "219", "220", 
		"221", "222", "223", "224", "225", "226", "227", "228", "229", "230", 
		"231", "232", "233", "234", "235", "236", "237", "238", "239", "240", 
		"241", "242", "243", "244", "245", "246", "247", "248", "249", "250", 
		"251", "252", "253", "254", "255",
	};
	/*
	 * 
	 */
	public static String yagoPath = "/user/root/yago2/";
	public static String spolPath = "/user/root/sopl/";
	public static String sub2IDPath = "/user/root/output/subID/";
	public static String obj2IDPath = "/user/root/output/objID/";
	public static String sub2Signature = "/user/root/sub2Sig_yago2/";
	public static String levelSignature = "/user/root/levelSig_yago2";
	public static String seqYagoPath = "/user/root/seqYago/";
	public static String generateIDPath = "/user/root/gID_yago2/";
	public static String IDyagoPath = "/user/root/IDyago_yago2/";
	public static String initCenters = "/user/root/InitCenters_yago2/";
	public static String ctr2subList = "/user/root/Centers2subList_yago2/";
	public static String ctrOutput = "/user/root/ctrOutput_yago2/";
	public static String checkID = "/user/root/checkID/";
	public static String summaryGraph = "/user/root/sumGraph_yago2/";
	public static String edgePair = "/user/root/edgePair/";
	public static String[] centerPath = new String[]{
		"/user/root/Center0/",
		"/user/root/Center1/",
		"/user/root/Center2/",
		"/user/root/Center3/",
		"/user/root/Center4/",
		"/user/root/Center5/"
		};
	/*
	 * 
	 */
	public static long subNum = 10557352;//;4339591
	public static long objNum = 24762725;//;8471558
	public static long preNum = 103;//91
	public static long locNum = 7701784;//;7394075
	public static int levelNum = -1;
	public static int nodeThreshold = 256;
	public static int SubScale = 15*1000*1000;
	public static int subEncodeLength = 120;
	public static int objEncodeLength = Signature.getLength() - subEncodeLength;
	public static int subMinLength = 3;
	public static int hashType = 1;
	public static globalConf dataChoice = globalConf.Yago2;
	public static int SigCloseDst = 15;//20;
	public static int[] centerFileNum = new int[]{18, 1, 1, 1, 1, 1, 1};
	//
	static int addID2Yago_splitMin = 128*1024*1024;
	static int addID2Yago_splitMax = 256*1024*1024;
	static int buildStree_splitMin = 16*1024*1024;
	
	static boolean buildSeqFile = 			false;
	static boolean deleteDuplicate = 		false;
	static boolean generateID = 			false;
	static boolean f_generateID = 			false;
	static boolean loadID = 				false;
	static boolean addID2yago = 			false;
	static boolean buildSignature = 		false;
	static boolean buildStree = 			false;
	static boolean findInitCenter = 		false;
	static boolean kMeans = 				false;
	static boolean loadLevel = 				false;
	static boolean buildSummaryGraph = 				true;
	static boolean smGraph	=				false;
	static boolean EdgeID 	= 						true;
	static boolean loadSumEdge =					true;
	static boolean checkIDTable = 			false;
	static boolean seq2Text = 				false;
	/*
	 * 
	 */	
	public static void main(String[] args) 
	throws  IOException, 
			InterruptedException, 
			ClassNotFoundException 
	{
		{
			offlineDriver.offlineConf();
			myHbase.initial();
		}
		
		if(buildSeqFile) bSeqFile();
		
		if(deleteDuplicate) delDup();
		
		if(generateID) gID();
		
		if(f_generateID) f_gID();
		
		if(loadID) lID();
		
		if(addID2yago) aID2yago();
		
		if(buildSignature) bSignature();
		
		if(buildStree) bStree();
		
		if(buildSummaryGraph) bSummaryGraph();
		{
			if(checkIDTable)			{				cIDTable();			}
			if(seq2Text) s2Text();
		}
		{
			myHbase.close();
		}
	}
	static boolean preMode = false;
	public static void bSummaryGraph() throws IOException
	{
		if(preMode){

			{
//				myHbase.createV2ETable();
//				myHbase.createV2VTable();
				myHbase.createSumEdgeTable();
				String _input, _output;
				{
					_input = offlineDriver.IDyagoPath;
					_output = offlineDriver.summaryGraph;
				}
				JobClient client = new JobClient();
				JobConf conf = new JobConf(offlineDriver.class);
				{
					conf.setJobName("summaryGraph");
					conf.setLong("subNum", offlineDriver.subNum);
					conf.setLong("levelNum", offlineDriver.levelNum);
				}
				// TODO: specify output types
				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(mEntry.class);

				// TODO: specify input and output DIRECTORIES (not files)
				{
					FileSystem fs = FileSystem.get(conf);
					if(fs.exists(new Path(_output)))
					{
						fs.delete(new Path(_output));
					}
				}
				FileInputFormat.addInputPath(conf, new Path(_input)); 
				FileOutputFormat.setOutputPath(conf, new Path(_output));
//				SequenceFileInputFormat.setInputPaths(conf, new Path(_input));
//				SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));

				conf.setMapperClass(summaryGraphMapper.class);
				conf.setReducerClass(summaryGraphReducer.class);
				conf.setNumMapTasks(56);
				conf.setNumReduceTasks(28);
				long splitSizeMin = 32 * 1024 * 1024;
				if(offlineDriver.dataChoice.equals(globalConf.Yago2)){
					splitSizeMin = 256 * 1024 * 1024;
				}
				conf.set("mapred.min.split.size", "" + splitSizeMin);
				conf.set("mapred.tasktracker.map.tasks.maximum", "4");
				conf.set("mapred.tasktracker.reduce.tasks.maximum", "2");

				client.setConf(conf);
				org.apache.hadoop.mapred.RunningJob _job = null;
				try {
					 _job = JobClient.runJob(conf);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return;
		}
		if(smGraph)
		{
			String _input, _output;
			{
				_input = offlineDriver.IDyagoPath;
				_output = offlineDriver.edgePair;
			}
			JobClient client = new JobClient();
			JobConf conf = new JobConf(offlineDriver.class);
			{
				conf.setJobName("smGraph");
				conf.setLong("subNum", offlineDriver.subNum);
				conf.setLong("levelNum", offlineDriver.levelNum);
			}
			// TODO: specify output types
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Signature.class);

			// TODO: specify input and output DIRECTORIES (not files)
			{
				FileSystem fs = FileSystem.get(conf);
				if(fs.exists(new Path(_output)))
				{
					fs.delete(new Path(_output));
				}
			}
			FileInputFormat.addInputPath(conf, new Path(_input)); 
			FileOutputFormat.setOutputPath(conf, new Path(_output));
//			SequenceFileInputFormat.setInputPaths(conf, new Path(_input));
//			SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));

			conf.setMapperClass(smGraphMapper.class);
			conf.setReducerClass(smGraphReducer.class);
			conf.setNumMapTasks(56);
			conf.setNumReduceTasks(7);
			long splitSizeMin = 32 * 1024 * 1024;
			if(offlineDriver.dataChoice.equals(globalConf.Yago2)){
				splitSizeMin = 256 * 1024 * 1024;
			}
			conf.set("mapred.min.split.size", "" + splitSizeMin);
			conf.set("mapred.tasktracker.map.tasks.maximum", "4");
			conf.set("mapred.tasktracker.reduce.tasks.maximum", "2");

			client.setConf(conf);
			org.apache.hadoop.mapred.RunningJob _job = null;
			try {
				 _job = JobClient.runJob(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
			myHbase.closeAll();
		}
		//
		if(EdgeID){
			String _input, _output;
			{
				_input = offlineDriver.edgePair;
				_output = offlineDriver.summaryGraph;
			}
			JobClient client = new JobClient();
			JobConf conf = new JobConf(offlineDriver.class);
			{
				conf.setJobName("EdgeID");
				conf.setLong("subNum", offlineDriver.subNum);
				conf.setLong("levelNum", offlineDriver.levelNum);
			}
			// TODO: specify output types
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			// TODO: specify input and output DIRECTORIES (not files)
			{
				FileSystem fs = FileSystem.get(conf);
				if(fs.exists(new Path(_output)))
				{
					fs.delete(new Path(_output));
				}
			}
			FileInputFormat.addInputPath(conf, new Path(_input)); 
			FileOutputFormat.setOutputPath(conf, new Path(_output));
//			SequenceFileInputFormat.setInputPaths(conf, new Path(_input));
//			SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));

			conf.setMapperClass(edgeIDMapper.class);
			conf.setReducerClass(edgeIDReducer.class);
			conf.setNumMapTasks(28);
			conf.setNumReduceTasks(28);
			long splitSizeMin = 32 * 1024 * 1024;
			if(offlineDriver.dataChoice.equals(globalConf.Yago2)){
				splitSizeMin = 200 * 1024 * 1024;
			}
			conf.set("mapred.min.split.size", "" + splitSizeMin);
			conf.set("mapred.tasktracker.map.tasks.maximum", "4");
			conf.set("mapred.tasktracker.reduce.tasks.maximum", "2");

			client.setConf(conf);
			org.apache.hadoop.mapred.RunningJob _job = null;
			try {
				 _job = JobClient.runJob(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
			myHbase.closeAll();
		
		}
		//
		if(loadSumEdge){
			{
				myHbase.createSumEdgeTable();
			}
			String _input, _output;
			{
				_input = offlineDriver.summaryGraph;
				_output = "/user/root/tempIO2";
			}
			JobClient client = new JobClient();
			JobConf conf = new JobConf(offlineDriver.class);
			{
				conf.setJobName("loadSumEdge");
				conf.setLong("subNum", offlineDriver.subNum);
				conf.setLong("levelNum", offlineDriver.levelNum);
			}
			// TODO: specify output types
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			// TODO: specify input and output DIRECTORIES (not files)
			{
				FileSystem fs = FileSystem.get(conf);
				if(fs.exists(new Path(_output)))
				{
					fs.delete(new Path(_output));
				}
			}
			FileInputFormat.addInputPath(conf, new Path(_input)); 
			FileOutputFormat.setOutputPath(conf, new Path(_output));
//			SequenceFileInputFormat.setInputPaths(conf, new Path(_input));
//			SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));

			conf.setMapperClass(loadSumEdgeMapper.class);
			conf.setReducerClass(loadSumEdgeReducer.class);
			conf.setNumMapTasks(56);
			conf.setNumReduceTasks(28);
			long splitSizeMin = 32 * 1024 * 1024;
			if(offlineDriver.dataChoice.equals(globalConf.Yago2)){
				splitSizeMin = 400 * 1024 * 1024;
			}
			conf.set("mapred.min.split.size", "" + splitSizeMin);

			client.setConf(conf);
			org.apache.hadoop.mapred.RunningJob _job = null;
			try {
				 _job = JobClient.runJob(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
			myHbase.closeAll();
		
		}
	}
	private static long numCenters = -1;
	public static void bStree() throws IOException
	{
		numCenters = offlineDriver.subNum;
		String _sig_path = null;
		int _sig_num = -1;
		int _level = 0;
		int nIterators = 5;
		int[] _iterator = new int[]{2, 2, 2, 1, 1, 1};
		int[] splitSize;
		if(offlineDriver.dataChoice.equals(globalConf.Yago)){
			splitSize = new int[]{16*1024*1024, 1*1024*1024, 1*1024*1024, 1*1024*1024, 1*1024*1024, 1*1024*1024};
		}
		else
		{
			splitSize = new int[]{128*1024*1024, 4*1024*1024, 1*1024*1024, 1*1024*1024, 1*1024*1024, 1*1024*1024};
		}
		
		{
			myHbase.createStreeTable();
		}
		while(true)
		{
			{
				nIterators = _iterator[_level];
				_sig_path = offlineDriver.curSigPath(_level);
				if(_level == 0) _sig_num = (int) offlineDriver.subNum;
			}
			{
				// <input,sig_path>  <output,init_c>
				offlineDriver.fInitCenter(_sig_path, initCenters, _sig_num / offlineDriver.nodeThreshold, splitSize[_level]);
			}
			{
				// <input,sig_path>  <output,center_path>  <load, pre_center_path>
				offlineDriver.kMeans(_sig_path, _level, nIterators, _sig_num, splitSize[_level]);
			}
			{
				// <input,sig_level>  <output,sig_level + 1> <load, center_path_level>
				offlineDriver.loadLevel(_sig_path, _level);
				_sig_num = (int) numCenters;// set the number of new centers in loadLevel;
			}
			_level ++;
			if(_sig_num <= offlineDriver.nodeThreshold) break;
		}
		offlineDriver.levelNum = _level;
	}
	public static void fInitCenter(String _sig_path, String _initc_path, int _c_num, int spiltSizeMin) throws IOException
	{
		String _input, _output;
		{
			_input = _sig_path;
			_output = _initc_path;
		}
		JobClient client = new JobClient();
		JobConf conf = new JobConf(offlineDriver.class);
		{
			conf.setJobName("fInitCenter_" + _sig_path);
			conf.setLong("centerNum", _c_num);
		}
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Signature.class);
		
		// TODO: specify input and output DIRECTORIES (not files)
		FileSystem fs = FileSystem.get(conf);
		{
			if(fs.exists(new Path(_output)))
			{
				fs.delete(new Path("/user/root/InitcTemp/"));
				fs.rename(new Path(_output), new Path("/user/root/InitcTemp/"));
			}
		}
		FileInputFormat.addInputPath(conf, new Path(_input)); 
		FileOutputFormat.setOutputPath(conf, new Path(_output));
//		SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));
		conf.setMapperClass(findCenterMapper.class);
		conf.setReducerClass(findCenterReducer.class);
		conf.setNumMapTasks(27);
		conf.setNumReduceTasks(1);// ever set it 9, unbelievable!!!
		conf.set("mapred.min.split.size", "" + spiltSizeMin);
		conf.set("mapred.tasktracker.map.tasks.maximum", "4");
		
		client.setConf(conf);
		org.apache.hadoop.mapred.RunningJob _job = null;
		try {
			 _job = JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void kMeans(String _sig_path, int _level, int nIterator, int _sig_num, int splitSize) throws IOException
	{
		String _input, _output;
		{
			_input = _sig_path;
			_output = ctrOutput;
		}
		for(int i = 0; i < nIterator; i ++)
		{		
			if(! offlineDriver.kMeans) break;
			JobClient client = new JobClient();
			JobConf conf = new JobConf(offlineDriver.class);
			{
				conf.setJobName(_level + "_kMeans_" + i);
				conf.setLong("iIterator", i);//i
				conf.setLong("level", _level);
				conf.setLong("subNum_center", _sig_num / offlineDriver.nodeThreshold);
			}
			// TODO: specify output types
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
	
			// TODO: specify input and output DIRECTORIES (not files)
			FileSystem fs = FileSystem.get(conf);
			
			FileInputFormat.setInputPaths(conf, new Path(_input));
			FileOutputFormat.setOutputPath(conf, new Path(_output));
//			SequenceFileInputFormat.addInputPath(conf, new Path(_input)); 
//			SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));
			{
				if(fs.exists(new Path(_output)))
				{
					fs.delete(new Path(_output));
				}
			}
			conf.setMapperClass(KmeansMapper.class);
			conf.setReducerClass(KmeansReducer.class);
			conf.setNumMapTasks(36);
			conf.setNumReduceTasks(offlineDriver.centerFileNum[_level]);
			int splitSizeMin = buildStree_splitMin;
			conf.set("mapred.tasktracker.map.tasks.maximum", "4");
			conf.set("mapred.tasktracker.reduce.tasks.maximum", "4");
			conf.set("mapred.min.split.size", "" + splitSizeMin);
			
			client.setConf(conf);
			org.apache.hadoop.mapred.RunningJob _job = null;
			try {
				 _job = JobClient.runJob(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
			{
				fs.delete(new Path(centerPath[_level]));
				fs.rename(new Path(_output), new Path(centerPath[_level]));
			}
		}
	}
	public static void loadLevel(String _sig_path, int _level) throws IOException
	{
		String tempIO = "/user/root/tempIO/";
		String _input = null, _output = null;
		{
			_input = _sig_path;
			_output = tempIO;
			JobClient client = new JobClient();
			JobConf conf = new JobConf(offlineDriver.class);
			{
				conf.setJobName("treeLevel_" + _level);
				conf.setLong("level", _level);
				conf.setLong("subNum", offlineDriver.subNum);
			}
			// TODO: specify output types
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			// TODO: specify input and output DIRECTORIES (not files)
			FileSystem fs = FileSystem.get(conf);
			{
				if(fs.exists(new Path(_output)))
				{
					fs.delete(new Path(_output));
				}
			}
			FileInputFormat.setInputPaths(conf, new Path(_input));
			FileOutputFormat.setOutputPath(conf, new Path(_output));
//			SequenceFileInputFormat.addInputPath(conf, new Path(kmInput)); 
//			SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));
			conf.setMapperClass(treeLevelMapper.class);
			conf.setReducerClass(treeLevelReducer.class);
			conf.setNumMapTasks(18);
			conf.setNumReduceTasks(9);
			conf.set("mapred.tasktracker.map.tasks.maximum", "2");
//			conf.set("mapred.min.split.size", "" + (256 * 1024 * 1024));
			
			client.setConf(conf);
			org.apache.hadoop.mapred.RunningJob _job = null;
			try {
				 _job = JobClient.runJob(conf);
				 Counters CT = _job.getCounters();
				 numCenters = CT.getCounter(oReport.CenterNum);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		/*
		 * load
		 */
		{
			_input = tempIO;
			_output = offlineDriver.nextSigPath(_level);
			JobClient client = new JobClient();
			JobConf conf = new JobConf(offlineDriver.class);
			{
				conf.setJobName("loadLevel_" + _level);
				conf.setLong("level", _level);
				conf.setLong("subNum", offlineDriver.subNum);
			}
			// TODO: specify output types
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			// TODO: specify input and output DIRECTORIES (not files)
			FileSystem fs = FileSystem.get(conf);
			{
				if(fs.exists(new Path(_output)))
				{
					fs.delete(new Path(_output));
				}
			}
			FileInputFormat.setInputPaths(conf, new Path(_input));
			FileOutputFormat.setOutputPath(conf, new Path(_output));
//			SequenceFileInputFormat.addInputPath(conf, new Path(kmInput)); 
//			SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));
			conf.setMapperClass(loadLevelMapper.class);
			conf.setReducerClass(loadLevelReducer.class);
			conf.setNumMapTasks(36);
			conf.setNumReduceTasks(9);
			conf.set("mapred.tasktracker.map.tasks.maximum", "4");
			
			client.setConf(conf);
			org.apache.hadoop.mapred.RunningJob _job = null;
			try {
				 _job = JobClient.runJob(conf);
				 Counters CT = _job.getCounters();
				 numCenters = CT.getCounter(oReport.CenterNum);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	public static void bSeqFile() throws IOException
	{
		String _input, _output;
		{
			_input = yagoPath;
			_output = seqYagoPath;
		}
		JobClient client = new JobClient();
		JobConf conf = new JobConf(offlineDriver.class);
		{
			conf.setLong("subNum", offlineDriver.subNum);
		}
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		{
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path(_output)))
			{
				fs.delete(new Path(_output));
			}
		}
		FileInputFormat.addInputPath(conf, new Path(_input)); 
//		FileOutputFormat.setOutputPath(conf, new Path(_output));
		SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));

		conf.setMapperClass(buildSeqFileMapper.class);
		conf.setReducerClass(buildSeqFileReducer.class);
		conf.setNumMapTasks(36);
		conf.setNumReduceTasks(1);

		client.setConf(conf);
		org.apache.hadoop.mapred.RunningJob _job = null;
		try {
			 _job = JobClient.runJob(conf);
			Counters CT = _job.getCounters();
			subNum = CT.getCounter(oReport.SubNumber);
			preNum = CT.getCounter(oReport.PRENumber);
			objNum = CT.getCounter(oReport.OBJNumber);
			locNum = CT.getCounter(oReport.LOCNumber);
			System.out.println("sub: " + subNum + "\t" + "pre: " + preNum + "\t" +
					"obj: " + objNum + "\t" + "loc: " + locNum);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void delDup() throws IOException
	{
		String _input, _output;
		{
			_input = seqYagoPath;
			_output = spolPath;
		}
		JobClient client = new JobClient();
		JobConf conf = new JobConf(offlineDriver.class);
		{
			conf.setLong("subNum", offlineDriver.subNum);
		}
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		{
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path(_output)))
			{
				fs.delete(new Path(_output));
			}
		}
//		FileInputFormat.addInputPath(conf, new Path(_input)); 
//		FileOutputFormat.setOutputPath(conf, new Path(_output));
		SequenceFileInputFormat.setInputPaths(conf, new Path(_input));
		SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));

		conf.setMapperClass(deleteDupMapper.class);
		conf.setReducerClass(deleteDupReducer.class);
		conf.setNumMapTasks(36);
		conf.setNumReduceTasks(9);
		long splitSize = 64 * 1024 * 1024;
		conf.set("mapred.min.split.size", "" + splitSize);
		conf.set("mapred.tasktracker.map.tasks.maximum", "4");

		client.setConf(conf);
		org.apache.hadoop.mapred.RunningJob _job = null;
		try {
			 _job = JobClient.runJob(conf);
			Counters CT = _job.getCounters();
			subNum = CT.getCounter(oReport.SubNumber);
			preNum = CT.getCounter(oReport.PRENumber);
			objNum = CT.getCounter(oReport.OBJNumber);
			locNum = CT.getCounter(oReport.LOCNumber);
			System.out.println("sub: " + subNum + "\t" + "pre: " + preNum + "\t" +
					"obj: " + objNum + "\t" + "loc: " + locNum);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void f_gID() throws IOException
	{
		String _input, _output;
		{
			_input = spolPath;
			_output = generateIDPath;
		}
		JobClient client = new JobClient();
		JobConf conf = new JobConf(offlineDriver.class);
		{
			conf.setLong("subNum", offlineDriver.subNum);
		}
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		{
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path(_output)))
			{
				fs.delete(new Path(_output));
			}
		}
		FileInputFormat.addInputPath(conf, new Path(_input)); 
		FileOutputFormat.setOutputPath(conf, new Path(_output));
//		SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));

		conf.setMapperClass(f_generateIDMapper.class);
		conf.setReducerClass(f_generateIDReducer.class);
		conf.setNumMapTasks(36);
		conf.setNumReduceTasks(9);
		int splitSize = 64 * 1024 * 1024;
		conf.set("mapred.min.split.size", "" + splitSize);
		client.setConf(conf);
		org.apache.hadoop.mapred.RunningJob _job = null;
		try {
			 _job = JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void lID() throws IOException
	{

		String _input, _output;
		{
			_input = generateIDPath;
			_output = "/user/root/temp/";
			myHbase.createIDtable();
		}
		JobClient client = new JobClient();
		JobConf conf = new JobConf(offlineDriver.class);
		{
			conf.setLong("subNum", offlineDriver.subNum);
		}
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		{
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path(_output)))
			{
				fs.delete(new Path(_output));
			}
		}
		FileInputFormat.addInputPath(conf, new Path(_input)); 
		FileOutputFormat.setOutputPath(conf, new Path(_output));
//		SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));

		conf.setMapperClass(loadIDMapper.class);
		conf.setReducerClass(loadIDReducer.class);
		conf.setNumMapTasks(36);
		conf.setNumReduceTasks(0);
		int maxSplitSize = 48*1024*1024;
		if(offlineDriver.dataChoice.equals(globalConf.Yago2)){
			maxSplitSize = 64*1024*1024;
		}
		conf.set("mapred.min.split.size", "" + (16 * 1024 * 1024));
		conf.set("mapred.max.split.size", "" + maxSplitSize);
		conf.set("mapred.tasktracker.map.tasks.maximum", "5");
		client.setConf(conf);
		org.apache.hadoop.mapred.RunningJob _job = null;
		try {
			 _job = JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		myHbase.closeAll();
	}
	public static void gID() throws IOException, InterruptedException, ClassNotFoundException
	{
		Job job = new Job(myHbase.getConf());
		job.setJarByClass(offlineDriver.class);
		String _input;
		{
			_input = spolPath;
//			myHbase.clearIDtable();
//			if(true )return;
			myHbase.createIDtable();
		}
		org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.addInputPath(job, new Path(_input));
		org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.setMinInputSplitSize(job, 32*1024*1024);
		org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.setMaxInputSplitSize(job, 256*1204*1024);
		job.getConfiguration().setLong("subNum", offlineDriver.subNum);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(generateIDMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(4);
		job.getConfiguration().set("mapred.tasktracker.reduce.tasks.maximum", "1");
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		job.setReducerClass(generateIDReducer.class);
		job.waitForCompletion(true);
	}
	public static void aID2yago() throws IOException
	{
		String _input, _output;
		{
			_input = seqYagoPath;
			_output = IDyagoPath;
		}
		JobClient client = new JobClient();
		JobConf conf = new JobConf(offlineDriver.class);
		{
			conf.setLong("subNum", offlineDriver.subNum);
		}
		// TODO: specify output types
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		{
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path(_output)))
			{
				fs.delete(new Path(_output));
			}
		}
		SequenceFileInputFormat.addInputPath(conf, new Path(_input)); 
		FileOutputFormat.setOutputPath(conf, new Path(_output));
//		SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));

		conf.setMapperClass(addID2yagoMapper.class);
		conf.setReducerClass(addID2yagoReducer.class);
		conf.setNumMapTasks(36);
		conf.setNumReduceTasks(1);
		int splitSizeMin = offlineDriver.addID2Yago_splitMin;//128
		int splitSizeMax = offlineDriver.addID2Yago_splitMin;//256
		conf.set("mapred.min.split.size", "" + splitSizeMin);
		conf.set("mapred.max.split.size", "" + splitSizeMax);
		conf.set("mapred.tasktracker.map.tasks.maximum", "6");
		
		client.setConf(conf);
		org.apache.hadoop.mapred.RunningJob _job = null;
		try {
			 _job = JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void bSignature() throws IOException
	{
		String _input, _output;
		{
			_input = IDyagoPath;
			_output = sub2Signature;
		}
		JobClient client = new JobClient();
		JobConf conf = new JobConf(offlineDriver.class);
		{
			conf.setLong("subNum", offlineDriver.subNum);
		}
		// TODO: specify output types
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Signature.class);

		// TODO: specify input and output DIRECTORIES (not files)
		{
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path(_output)))
			{
				fs.delete(new Path(_output));
			}
		}
//		SequenceFileInputFormat.addInputPath(conf, new Path(_input)); 
		FileInputFormat.setInputPaths(conf, new Path(_input));
		FileOutputFormat.setOutputPath(conf, new Path(_output));

		conf.setMapperClass(buildSigMapper.class);
		conf.setReducerClass(buildSigReducer.class);
		conf.setNumMapTasks(40);
		conf.setNumReduceTasks(1);
		int splitSizeMin = 32*1024*1024;//128
		if(offlineDriver.dataChoice.equals(globalConf.Yago2)){
			splitSizeMin = 320*1024*1024;
		}
		conf.set("mapred.min.split.size", splitSizeMin + "");
		conf.set("mapred.tasktracker.map.tasks.maximum", "4");

		client.setConf(conf);
		org.apache.hadoop.mapred.RunningJob _job = null;
		try {
			 _job = JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		myHbase.closeAll();
	}
	public static void s2Text() throws IOException
	{
		String _input, _output;
		{
			_input = "/user/root/temp/";
			_output = "/user/root/sub2ID/";
		}
		JobClient client = new JobClient();
		JobConf conf = new JobConf(offlineDriver.class);

		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		{
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path(_output)))
			{
				fs.delete(new Path(_output));
			}
		}
//		FileInputFormat.addInputPath(conf, new Path(_input)); 
//		FileOutputFormat.setOutputPath(conf, new Path(_output));
		SequenceFileInputFormat.setInputPaths(conf, new Path(_input));
		SequenceFileOutputFormat.setOutputPath(conf, new Path(_output));

		conf.setMapperClass(seq2TextMapper.class);
		conf.setReducerClass(set2TextReducer.class);
		conf.setNumMapTasks(36);
		conf.setNumReduceTasks(9);

		client.setConf(conf);
		org.apache.hadoop.mapred.RunningJob _job = null;
		try {
			 _job = JobClient.runJob(conf);
			Counters CT = _job.getCounters();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void cIDTable() throws IOException, InterruptedException, ClassNotFoundException
	{
		Job job = new Job(myHbase.getConf());
		job.setJarByClass(offlineDriver.class);
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		String _output;
		{
			_output = offlineDriver.checkID;
			FileSystem fs = FileSystem.get(new JobConf(offlineDriver.class));
			{
				if(fs.exists(new Path(_output)))
				{
					fs.delete(new Path(_output));
				}
			}
		}
		TableMapReduceUtil.initTableMapperJob(
				offlineDriver.ID2sub,        // input table
				scan,               // Scan instance to control CF and attribute selection
				Table2FileMapper.class,     // mapper class
				Text.class,         // mapper output key
				Text.class,  // mapper output value
				job);
		org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.setMinInputSplitSize(job, 32*1024*1024);
		org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.setMaxInputSplitSize(job, 256*1204*1024);
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(_output));
		job.setReducerClass(Table2FileReducer.class);
		job.setNumReduceTasks(9);
		job.getConfiguration().set("mapred.tasktracker.reduce.tasks.maximum", "2");
//		job.setReducerClass(generateIDReducer.class);
		/*
		 * ��鳰���������
		 */
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
    public static boolean existPath(String _path) throws IOException
    {
		FileSystem fs = FileSystem.get(new JobConf());
		return fs.exists(new Path(_path));
    }
	public boolean deleteOutput(JobConf _conf, String _output) throws IOException
	{
		FileSystem fs = FileSystem.get(_conf);
		if(fs.exists(new Path(_output)))
		{
			fs.delete(new Path(_output));
			return true;
		}
		return false;
	}
	public static String encodeEdge(int _in, int _out)
	{
//		StringBuffer _sb = new StringBuffer(_in).append(" ").append(_out);  bug!!
		StringBuffer _sb = new StringBuffer();
		_sb.append(_in).append(" ").append(_out);  
		return _sb.toString();
	}
	public static void offlineConf()
	{
		if(offlineDriver.dataChoice.equals(globalConf.Yago))
		{
			offlineDriver.YagoConf();
		}
		else
		{
			offlineDriver.Yago2Conf();
		}
	}
	private static void YagoConf()
	{	
		subNum = 4339591;
		objNum = 8471558;
		preNum = 91;
		levelNum = 3;
		nodeThreshold = 256;
		SubScale = (int) subNum;
		subEncodeLength = 120;
		objEncodeLength = Signature.getLength() - subEncodeLength;
		subMinLength = 3;
		hashType = 1;
		SigCloseDst = 15;//20;
		centerFileNum = new int[]{18, 1, 1, 1, 1, 1, 1};
		//
		addID2Yago_splitMin = 32*1024*1024;
		addID2Yago_splitMax = 64*1024*1024;
		buildStree_splitMin = 16*1024*1024;
	}
	private static void Yago2Conf()
	{	
		subNum = 10557352;
		objNum = 24762725;
		preNum = 103;
		locNum = 7701784;
		levelNum = 4;
		nodeThreshold = 256;
		SubScale = (int) subNum;
		subEncodeLength = 120;
		objEncodeLength = Signature.getLength() - subEncodeLength;
		subMinLength = 3;
		hashType = 1;
		SigCloseDst = 80;//20;
		centerFileNum = new int[]{18, 1, 1, 1, 1, 1, 1};
		//
		addID2Yago_splitMin = 128*1024*1024;
		addID2Yago_splitMax = 256*1024*1024;
		buildStree_splitMin = 32*1024*1024;
	}
	private static void checkDist()
	{
		Signature _s1 = new Signature("14 26 34 74 216 224 ", false);
		Signature _s2 = new Signature("47 53 57 63 83 89 99 107 113 142 270 ", false);
		System.out.print(_s1.dist(_s2) + "\n");
		System.out.print(_s1.bitToString() + "\n");
		System.out.print(_s2.bitToString() + "\n");
	}
	private static void checkSignature()
	{

		String _mLine = "1199999	33	29947830	5947511	geoent_Dastah_170848	isCalled	\"Deste\"	geoent_Dastah_170848\n" +
				"1199999	33	17569480	5947511	geoent_Dastah_170848	isCalled	\"Dest\\u00e9\"	geoent_Dastah_170848\n" +
				"1199999	75	11285261	5947511	geoent_Dastah_170848	hasGeoCoordinates	33.21667/36.23333	geoent_Dastah_170848\n" +
				"1199999	56	8206420	5947511	geoent_Dastah_170848	type_star	wordnet_object_100002684	geoent_Dastah_170848\n" +
				"1199999	30	30592036	-1	geoent_Dastah_170848	hasLongitude	36.23333	----\n" +
				"1199999	56	10552486	5947511	geoent_Dastah_170848	type_star	yagoPermanentlyLocatedEntity	geoent_Dastah_170848\n" +
				"1199999	16	34671972	-1	geoent_Dastah_170848	hasLatitude	33.21667	----\n" +
				"1199999	56	5056944	5947511	geoent_Dastah_170848	type_star	wordnet_point_108620061	geoent_Dastah_170848\n" +
				"1199999	56	8793883	5947511	geoent_Dastah_170848	type_star	wordnet_workplace_104602044	geoent_Dastah_170848\n" +
				"1199999	56	1168172	5947511	geoent_Dastah_170848	type_star	yagoGeoEntity	geoent_Dastah_170848\n" +
				"1199999	56	4102397	5947511	geoent_Dastah_170848	type_star	yagoLegalActorGeo	geoent_Dastah_170848\n" +
				"1199999	56	8792654	5947511	geoent_Dastah_170848	type_star	wordnet_physical_entity_100001930	geoent_Dastah_170848\n" +
				"1199999	56	8792167	5947511	geoent_Dastah_170848	type_star	wordnet_location_100027167	geoent_Dastah_170848\n" +
				"1199999	56	9962858	5947511	geoent_Dastah_170848	type_star	wordnet_geographic_point_108578706	geoent_Dastah_170848\n" +
				"1199999	56	5860946	5947511	geoent_Dastah_170848	type_star	wordnet_farm_103322099	geoent_Dastah_170848\n" +
				"1199999	56	4685935	5947511	geoent_Dastah_170848	type_star	wordnet_entity_100001740	geoent_Dastah_170848\n" +
				"1199999	36	9130967	5947511	geoent_Dastah_170848	type	geoclass_farm	geoent_Dastah_170848\n";

		String[] _line = _mLine.split("\n");
		Signature iSig = new Signature();
		{
			for(int i = 0; i < _line.length; i ++)
				tmpBuildSig(iSig, _line[i]);
		}
		System.out.print(iSig.bitToString() + "\t---\n");
	}
	private static void tmpBuildSig(Signature iSig, String _line)
	{
		String [] rdfUnit = _line.split("\t");
		/*
		 * 
		 */		
		int subID = -1, objID = -1, preID = -1, locID = -1;
		{
			subID = Integer.parseInt(rdfUnit[0]);
			preID = Integer.parseInt(rdfUnit[1]);
			objID = Integer.parseInt(rdfUnit[2]);
			locID = Integer.parseInt(rdfUnit[3]);
		}
		/*
		 * 
		 */
		/*
		 * 
		 */
		{
//			iSig = new Signature();
			{
				iSig.set(subID);
				iSig.set(preID);
				iSig.set(objID);
			}
//			offlineDriver.buildLocSignature(rdfUnit[7], locID, iSig, 1);
//			offlineDriver.buildPreSignature(preID, iSig, 1);
//			offlineDriver.buildEntitySignature(rdfUnit[6], iSig, offlineDriver.hashType, 1);
		}
		/*
		 * 
		 */
		if(objID < subNum)
		{
//			iSig = new Signature();
//			offlineDriver.buildLocSignature(rdfUnit[7], locID, iSig, 0);
//			offlineDriver.buildPreSignature(preID, iSig, 0);
//			offlineDriver.buildEntitySignature(rdfUnit[4], iSig, offlineDriver.hashType, 0);
//			iSig = null;
		}
	}
    public static void buildLocSignature(String _loc, int _locID, Signature _sig, final int offset)
    {
    	//printf("buildLocationSignature..%s\n",_loc.c_str());
    	final int SIGNATURE_LENGTH_O = offlineDriver.objEncodeLength;
    	final int SIGNATURE_LENGTH_S = offlineDriver.subEncodeLength;
    	final int SUBLEN = offlineDriver.subMinLength;
    	if(_loc.length() >0 && _loc.charAt(0)=='-'){
    		return;
    	}
    	if (_loc.charAt(0)=='('){//
    		double x1;
    		double x2;
    		double y1;
    		double y2;
    		//sscanf_s(_loc.c_str(),"([%lf,%lf],[%lf,%lf])",&x1,&x2,&y1,&y2);
    		//printf("read:\n([%lf,%lf],[%lf,%lf])\n",x1,x2,y1,y2);
    		{
    			String[] _sp = _loc.split("\\(|,|\\)|=|}");
    			x1 = Double.parseDouble(_sp[2]);
    			x2 = Double.parseDouble(_sp[3]);
    			y1 = Double.parseDouble(_sp[6]);
    			y2 = Double.parseDouble(_sp[7]);
    		}
    		locationHash(-90,90,x1,x2,_sig,false);
    		locationHash(-180,180,y1,y2,_sig,true);
    	}
    	else if (_loc.charAt(0)=='{'){//
    		double x1;
    		double x2;
    		double y1;
    		double y2;
    		double x,y,r;
    		//sscanf_s(_loc.c_str(),"{O=(%lf,%lf),r=%lf}",&x,&y,&r);
    		//printf("read:\n([%lf,%lf],[%lf,%lf])\n",x1,x2,y1,y2);
    		{
    			String[] _sp = _loc.split("\\(|,|\\)|=|}");
    			x = Double.parseDouble(_sp[2]);
    			y = Double.parseDouble(_sp[3]);
    			r = Double.parseDouble(_sp[6]);
    		}
    		x1=x-r;
    		x2=x+r;
    		y1=y-r;
    		y2=y+r;
    		locationHash(-90,90,x1,x2,_sig,false);
    		locationHash(-180,180,y1,y2,_sig,true);
    	}
    	else {//
    		//printf("buildLocationSignature..%s\n",_loc.c_str());
    		int length = (int) _loc.length();
    		if(length >=SUBLEN)
    		{
    			for(int i=0; i<=length-SUBLEN; i++)
    			{
    				String testsubstring = _loc.substring(i, i + SUBLEN);
    				int hashvalue = BKDRHash(testsubstring);
    				int pos = hashvalue % (SIGNATURE_LENGTH_O)+SIGNATURE_LENGTH_S;
    				_sig.set(pos);
    			}
    		}
    		double locX,locY;
    		if(_locID == -1) return;
    		
    		String _xy = myHbase.getXYByLocID(_locID);
    		String[] _sp = _xy.split("/");
    		locX = Double.parseDouble(_sp[0]);
    		locY = Double.parseDouble(_sp[1]);
    		locationHash(-90,90,locX,locX,_sig,false);
    		locationHash(-180,180,locY,locY,_sig,true);
    	}
}
    public static void buildPreSignature(int preID, Signature _sig, final int offset)
    {
    	int SIGNATURE_LENGTH_O = offlineDriver.objEncodeLength;
    	int SIGNATURE_LENGTH_S = offlineDriver.subEncodeLength;
    	int seed=3;
    	int pos=(preID+offset)%SIGNATURE_LENGTH_O+SIGNATURE_LENGTH_S;
    	int pos2=(preID*preID+preID*seed+offset)%SIGNATURE_LENGTH_O+SIGNATURE_LENGTH_S;
    	_sig.set(pos);
    	_sig.set(pos2);
    	return;
    }
    public static void buildEntitySignature(String _entity, Signature _sig, int hashType, int offset)
    {// typeofHashFun: 1. BKDRHash hash function; 2
    	if(_entity.length() > 0 && _entity.charAt(0)=='?')
    		return;
    	int length = _entity.length();
    	int SUBLEN = offlineDriver.subMinLength;
    	int SIGNATURE_LENGTH_S = offlineDriver.subEncodeLength;
    	if (hashType==0){
    		if(length >=SUBLEN)
    		{
    			for(int i=0; i<=length-SUBLEN; i++)
    			{
    				String testsubstring = _entity.substring(i, i + SUBLEN);
    				int hashvalue = BKDRHash(testsubstring);
    				int pos = (hashvalue+offset) % (SIGNATURE_LENGTH_S);
    				_sig.set(pos);
    			}
    		}
    	}
    	else if (hashType==1){
    		final int seedNum=10;
    		int[] seed =new int[]{2,3,5,7,11,13,17,19,23,29};
    		long pos=0;
    		for (int j=0;j<seedNum;j++){
    			pos=0;
    			for(int i=0; i<=length; i++){
    				pos=(pos*seed[j]+_entity.charAt(0)+offset) % SIGNATURE_LENGTH_S;
    			}
    			_sig.set((int) pos);
    		}
    	}
    }
    public static void locationHash(double a,double b,double key1,double key2,
    		Signature _sig, boolean transfer,final int offset)
    {
    	int SIGNATURE_LENGTH_O = offlineDriver.objEncodeLength;
    	int SIGNATURE_LENGTH_S = offlineDriver.subEncodeLength;
    	double value1=(key1-a)/(b-a);
    	double value2=(key2-a)/(b-a);
    	int base1=2;
    	int base2=2;
    	boolean transfer1=true;
    	boolean transfer2=true;
    	_sig.set(1);
    	for (int i=0;i<7;i++){
    		value1*=2;
    		value2*=2;
    		if (value1<1){
    			base1=base1*2-1;
    		}
    		else{
    			value1-=1;
    			base1=base1*2;
    			transfer1=false;
    		}
    		if (value2<1){
    			base2=base2*2-1;
    		}
    		else{
    			value2-=1;
    			base2=base2*2;
    			transfer1=false;
    		}
    		if (base1==base2){
    			_sig.set((base1+offset)%SIGNATURE_LENGTH_O+SIGNATURE_LENGTH_S);
    			if (transfer){
    				if (transfer1){
    					_sig.set(((base1-1)*2+offset)%SIGNATURE_LENGTH_O+SIGNATURE_LENGTH_S);
    				}
    				else{
    					_sig.set((base1-1+offset)%SIGNATURE_LENGTH_O+SIGNATURE_LENGTH_S);
    				}
    			}
    			else{
    					_sig.set((base1-1+offset)%SIGNATURE_LENGTH_O+SIGNATURE_LENGTH_S);
    			}
    		}
    		else if (base1==base2-1){
    			_sig.set((base1+offset)%SIGNATURE_LENGTH_O+SIGNATURE_LENGTH_S);
    		}
    		else if (base1==2*(base2-1)){
    			if (transfer){
    				_sig.set((base2+offset)%SIGNATURE_LENGTH_O+SIGNATURE_LENGTH_S);
    			}
    		}
    	}
    	return;
    }
    public static void locationHash(double a,double b,double key1,double key2,
    		Signature _sig, boolean transfer)
    {
    	final int offset = 0;
    	locationHash(a, b, key1, key2, _sig, transfer, offset);
    }
    public static int BKDRHash(String _s)
    {
    	long seed = 131; // 31 131 1313 13131 131313 etc..
    	long hash = 0;

    	for(int i = 0; i < _s.length(); i ++)
    	{
    		hash = hash * seed + _s.charAt(i);
    	}

    	return (int)(hash & 0x7FFFFFFF);
    }
	public static String curSigPath(int _cur_level)
	{
		if(_cur_level == 0) return sub2Signature;
		else
			return levelSignature + (_cur_level) + "/"; 
	}
	public static String nextSigPath(int _cur_level)
	{
		return offlineDriver.curSigPath(_cur_level + 1);
	}
}