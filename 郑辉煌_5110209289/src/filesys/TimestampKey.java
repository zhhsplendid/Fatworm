package filesys;

import java.sql.Timestamp;

import util.Lib;

public class TimestampKey extends BKey {

	public Timestamp value;
	
	public TimestampKey(Timestamp v){
		value = v;
	}
	
	public TimestampKey(Long v){
		value = new Timestamp(v);
	}
	
	@Override
	public int compareTo(BKey o) {
		return value.compareTo(((TimestampKey)o).value);
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof TimestampKey){
			return value.equals(((TimestampKey)o).value);
		}
		return false;
	}

	@Override
	public void remove() throws Throwable {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] toByte() throws Throwable {
		return Lib.longToByte(value.getTime());
	}

}
