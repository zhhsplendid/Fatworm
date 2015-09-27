package fatworm.driver;

public class EmptyCursor implements Cursor {
	public long time;
	public EmptyCursor(){
		
	}
	
	@Override
	public void beforeFirst() throws Throwable {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean notEnd() throws Throwable {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void next() throws Throwable {
		// TODO Auto-generated method stub

	}

	@Override
	public void prev() throws Throwable {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete() throws Throwable {
		// TODO Auto-generated method stub

	}

	@Override
	public Object getObject(String col) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] getObject() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record getRecord() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

}
