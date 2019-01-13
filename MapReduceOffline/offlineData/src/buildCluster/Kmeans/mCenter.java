package buildCluster.Kmeans;

import mapreduce.Signature;

public class mCenter 
{
	private String sub;// String.ValueOf(subID)
	private Signature sig;
	public mCenter(String _sub, Signature _sig) {
		setSub(_sub);
		setSig(_sig);
	}
	
	public mCenter(String _sub_sig, boolean _b)
	{
		String[] _sp = _sub_sig.split("\t");
		sub = _sp[0];
		sig = new Signature(_sp[1], _b);
	}
	
	public mCenter(String _sub_sigString)
	{
		String[] _sp = _sub_sigString.split("\t");
		sub = _sp[0];
		sig = new Signature(_sp[1]);
	}
	
	public String toString()
	{
		StringBuffer _sBuffer = new StringBuffer(sub);
		_sBuffer.append("\t").append(sig.toString());
		return _sBuffer.toString();
	}
	
	private void OR(mCenter _m)
	{
		sig.or(_m.getSig());
	}
	
	public int[] getSigVector()
	{
		return sig.getVector();
	}
	
	public int dist(mCenter _c)
	{
		int _ret = sig.dist(_c.getSig());
		return _ret;
	}
	public String getSub() {
		return sub;
	}
	public void setSub(String sub) {
		this.sub = sub;
	}
	public Signature getSig() {
		return sig;
	}
	public void setSig(Signature sig) {
		this.sig = sig;
	}
}
