package filesys;

public interface PageInterface {
	
	public Integer getID();
	public Long getTime();
	public int headerSize();
	public boolean isPartial();
	public int remainSize();
	public void deleteMark();
	
	
	public void flush() throws Throwable;
	public void fromBytes(byte []b) throws Throwable;
	public byte[] toBytes() throws Throwable;

}
