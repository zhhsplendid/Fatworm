package scan;

public interface ScanInterface {
	
	public void beforeFirst();
	public boolean hasNext();
	
	
	public abstract Object getObjectByIndex(int index);
	public int getTypeByIndex(int index);
	
	public int getColumnCount();
	
}
