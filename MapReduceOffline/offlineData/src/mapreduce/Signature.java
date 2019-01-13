package mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.Scanner;
import org.apache.hadoop.io.Writable;



public class Signature implements Writable
{
	private final static int longNum = 5;
	private final static int longSize = Long.SIZE;
	public final static int length = longNum * longSize;
	private long[] sig_bits = null;
	public Signature() {
		// TODO Auto-generated constructor stub
		{
			sig_bits = new long[longNum];
		}
		for(int i = 0; i < longNum; i ++)
		{
			sig_bits[i] &= 0x0000000000000000;
		}
	}
	
	public Signature(String _s){
		Scanner _sc = new Scanner(_s);
		{
			sig_bits = new long[longNum];
		}
		for(int i = 0; i < longNum; i ++)
		{
			sig_bits[i] = _sc.nextLong();
		}
	}
	
	public Signature(String _s, boolean _bit)
	{
		Scanner _sc = new Scanner(_s);
		{
			sig_bits = new long[longNum];
		}
		for(int i = 0; i < longNum; i ++)
		{
			sig_bits[i] &= 0x0000000000000000;
		}
		while(_sc.hasNextInt())
		{
			this.set(_sc.nextInt());
		}
	}
	
	public int dist(final Signature _s)
	{
		final long[] _s_bit = _s.getLongs();
		int _ret = 0;
		for(int i = 0; i < longNum; i ++)
		{
			long _l = _s_bit[i] ^ this.sig_bits[i];
			for(int i1 = 0; i1 < longSize; i1 ++)
			{
				if((_l & (1L << i1)) != 0)
					_ret ++;
			}
		}
		return _ret;
	}
	
	public long[] getLongs()
	{
		return sig_bits;
	}
	
	public Signature getClone()
	{
		Signature _ret = new Signature();
		_ret.or(this);
		return _ret;
	}
	
	public void or(final Signature _s)
	{
		final long[] sBit = _s.getLongs();
		for(int i = 0; i < longNum; i ++)
		{
			sig_bits[i] |= sBit[i];
		}
	}
	
	public void set(int _pos)
	{
		int _index = _pos % length;
		int iLong = _index / longSize;
		sig_bits[iLong] |= 1L << (_index - iLong * longSize);
	}
	
	public static int getLength()
	{
		return length;
	}
	
	public int ones()
	{
//		return bs.cardinality();
		return dist(new Signature());
	}
	
	public boolean isSet(int _i)
	{
		int _index = _i % length;
		int iLong = _index / longSize;
		return (sig_bits[iLong] & 
				(1L << (_index - iLong * longSize))
				) 
				!= 0;
	}
	
	public String bitToString()
	{
		StringBuffer _sb = new StringBuffer("");
		for(int i = 0; i < length; i ++)
		{
			if(isSet(i))	_sb.append(i).append(" ");
		}
		return _sb.toString();
	}
	
	public int[] getVector()
	{
		int[] _v = new int[getLength()];
		for(int i = 0; i < length; i ++)
		{
			if(isSet(i))	_v[i] = 1;
			else			_v[i] = 0;
		}
		return _v;
	}
	
	public String toString()
	{
		StringBuilder sb = new StringBuilder("");
		sb.append(sig_bits[0]);
		for(int i = 1; i < longNum; i ++)
		{
			sb.append(" ").append(sig_bits[i]);
		}
		return sb.toString();
	}
	public boolean sigEqual(Signature _s)
	{
		final long[] _l = _s.getLongs();
		for(int i = 0; i < longNum; i ++)
		{
			if(this.sig_bits[i] != _l[i]) return false;
		}
		return true;
	}
	public void clear()
	{
		for(int i = 0; i < longNum; i ++)
		{
			sig_bits[i] &= 0x0000000000000000;
		}
	}
	
	@Override
	public void readFields(DataInput ip) throws IOException {
		// TODO Auto-generated method stub
		{
			this.sig_bits = new long[longNum];// 
		}
		for(int i = 0; i < longNum; i ++)
		{
			this.sig_bits[i] = ip.readLong();
		}
	}

	@Override
	public void write(DataOutput op) throws IOException {
		// TODO Auto-generated method stub
		for(int i = 0; i < longNum; i ++)
		{
			op.writeLong(this.sig_bits[i]);
		}
	}

}
