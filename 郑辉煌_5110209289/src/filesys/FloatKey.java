package filesys;

import util.Lib;

public class FloatKey extends BKey {

	public float value;
	public FloatKey(float v){
		value = v;
	}
	@Override
	public int compareTo(BKey o) {
		float v = ((FloatKey)o).value;
		if(v < value) return 1;
		if(value < v) return -1;
		return 0;
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof FloatKey){
			return value == ((FloatKey)o).value;
		}
		return false;
	}

	@Override
	public void remove() throws Throwable {
		// TODO Auto-generated method stub
	}

	@Override
	public byte[] toByte() throws Throwable {
		return Lib.floatToBytes(value);
	}

}
