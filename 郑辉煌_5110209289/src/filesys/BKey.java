package filesys;


public abstract class BKey implements Comparable<BKey>{
	
	public abstract int compareTo(BKey o);
	public abstract boolean equals(Object o);
	public abstract void remove() throws Throwable;
	public abstract byte[] toByte() throws Throwable;
	
	public int keySize(){
		return Integer.SIZE / Byte.SIZE;
	}
	
	
}
