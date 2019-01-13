package summaryGraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.sun.xml.bind.v2.runtime.RuntimeUtil.ToStringAdapter;

import mapreduce.Signature;

public class mEntry implements Writable
{
	private Signature sig = null;
	private int id = -1;
	
	public mEntry()
	{
		id = -1;
		sig = null;
	}
	public mEntry(String _entry_str)
	{
		String[] _sp = _entry_str.split("\t");
		id = Integer.parseInt(_sp[0]);
		sig = new Signature(_sp[1]);
	}
	public mEntry(int _id, Signature _sig)
	{
		id = _id;
		sig = _sig;
	}
	public void setValue(int _id, Signature _sig){
		id = _id;
		sig = _sig;
	}
	public String toString()
	{
		StringBuffer _sb = new StringBuffer();
		_sb.append(id).append("\t").append(sig.toString());
		return _sb.toString();
	}
	public mEntry clone()
	{
		mEntry _ret = new mEntry(-1, new Signature());
		_ret.getSig().or(sig);
		_ret.setId(id);
		return _ret;
	}
	public Signature getSig() {
		return sig;
	}
	public void setSig(Signature sig) {
		this.sig = sig;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public void Print()
	{
		System.out.print(id + ":\t" + sig.toString() + "\n");
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		sig = new Signature();
		sig.readFields(arg0);
		id = arg0.readInt();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		sig.write(arg0);
		arg0.writeInt(id);
	}
}
