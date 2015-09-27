package filesys;

import util.Lib;


public class IntKey extends BKey {
	public long time;
	public int value;
	
	public IntKey(int v){
		value = v;
	}
	
	@Override
	public int compareTo(BKey o) {
		int v = ((IntKey)o).value;
		if(v < value) return 1;
		if(v > value) return -1;
		return 0;
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof IntKey){
			return value == ((IntKey)o).value;
		}
		return false;
	}

	@Override
	public void remove() {}

	@Override
	public byte[] toByte() throws Throwable {
		return Lib.intToBytes(value);
	}
	
}
