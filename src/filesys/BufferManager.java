package filesys;

import java.util.*;

import fatworm.driver.*;
import output.Debug;


public class BufferManager {
	
	public long time;
	public MyFile dataFile;
	public FreeList freeList;
	public Map<Integer, Page> pageMap = new TreeMap<Integer, Page>();
	public String fileName;
	public int writeCount = 0;
	
	private void addCount(){
		++writeCount;
	}
	
	public TreeSet<Page> LRUqueue = new TreeSet<Page>(new Comparator<Page>(){
		public int compare(Page p1, Page p2){
			Long time1 = p1.getTime();
			Long time2 = p2.getTime();
			return time1.compareTo(time2);
		}
	});
	
	public BufferManager(int fileType, String fileName) {
		this.fileName = fileName;
		dataFile = new MyFile(fileName, fileType);
		freeList = FreeList.load(fileName);
		freeList.save(fileName);
	}
	
	
	
	public void close() throws Throwable {
		
		ArrayList<Page> tmp = new ArrayList<Page>(pageMap.values());
		int size = tmp.size();
		for(int i = 0; i < size; ++i){
			Page p = tmp.get(i);
			p.flush();
			pageMap.remove(p.getID());
		}
		
		for(Page p: pageMap.values()){
			p.flush();
		}
		
		dataFile.close();
		freeList.save(fileName);
	}
	
	
	
	public synchronized void markDelPage(Integer pageId){
		Page toMark = pageMap.get(pageId);//
		if(toMark != null){
			pageMap.remove(pageId);
			LRUqueue.remove(toMark);
			toMark.deleteMark();	
		}
		freeList.add(pageId);
	}
	
	public BTreePage getBTreePage(int pageId, int type, BKeyManager btree, boolean create) throws Throwable{
		Page tmp = manageGetPage(pageId);
		boolean canReturn = tmp != null; //maybe wront so I write this way
		if(canReturn) {
			return (BTreePage)tmp;
		}
		BTreePage newPage = new BTreePage(btree, pageId, type, dataFile, create);
		LRUqueue.add(newPage);
		pageMap.put(pageId, newPage);
		return newPage;
	}
	
	public RecordPage getRecordPage(int pageId, boolean create) throws Throwable{
		Page tmp = manageGetPage(pageId);
		boolean canReturn = tmp != null; //maybe wront so I write this way
		if(canReturn) {
			return (RecordPage)tmp;
		}
		RecordPage newPage = new RecordPage(this, pageId, dataFile, create);
		LRUqueue.add(newPage);
		pageMap.put(pageId, newPage);
		return newPage;
	}
	
	/**
	 * get page not_or assert there will be enough space in memory
	 * @param pageId
	 * @return
	 * @throws Throwable 
	 */
	public synchronized Page manageGetPage(int pageId) throws Throwable{
		if(pageMap.containsKey(pageId)){
			return pageMap.get(pageId);
		}
		DatabaseEngine instance = DatabaseEngine.getInstance();
		while(instance.outOfMemory()){
			instance.flushOtherManager(this);
		}
		return null;
	}
	
	public synchronized boolean flushOutOnePage() throws Throwable{
		synchronized(LRUqueue){
			addCount(); // worning
			if(LRUqueue.isEmpty()){
				return false;
			}
			Page victim = null;
			Iterator<Page> iter = LRUqueue.iterator();
	
			while(iter.hasNext()){
				victim = iter.next();
				//if(!victim.inWriting()){ // !victim.isInTransaction() || victim.isEmpty()
					break;
				//}
			}
			iter.remove();
			//if(victim.inWriting()){
			//	Debug.warn("flushing out a page still in transaction, will try to recover.");
			//}
			victim.flush();
			pageMap.remove(victim.getID());
			return true;
		}
	}



	public Page getPage(int pageId, boolean create) throws Throwable {
		Page tmp = manageGetPage(pageId);
		boolean canReturn = tmp != null;
		if(canReturn) {
			return (Page)tmp;
		}
		Page page = new Page(pageId, dataFile, create);
		LRUqueue.add(page);
		pageMap.put(pageId, page);
		return page;
	}
	
	public synchronized Integer newPage(){
		return freeList.getOnePage();
	}
	
	public Page newPage(int size) throws Throwable {
		Integer ans = Page.newPageFromPool(size);
		if(ans != null){
			Page page = getPage(ans, false);
			page.newEntry();
			return page;
		}
		else{
			ans = newPage();
			return getPage(ans, true);
		}
	}
	
	public Integer getNextPage(Integer pageID) throws Throwable {
		return getRecordPage(pageID, false).nextID;
	}

	public Integer getPrevPage(Integer pageID) throws Throwable {
		return getRecordPage(pageID, false).preID;
	}
	
	public List<Record> getRecords(Integer pageId, Schema schema) {
		if(pageId == null || pageId < 0){
			return new ArrayList<Record>();
		}
		RecordByte buff = new RecordByte();
		ArrayList<Record> ans = new ArrayList<Record>();
		
		try {
			RecordPage page = getRecordPage(pageId, false);
			boolean notPartial = !page.isPartial();
			if(notPartial){
				return page.getRecords(schema);
			}
			
			combineRecord(buff, page);
			
			ans.add(Record.fromByte(buff.getBytes(), schema));
			
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}



	private void combineRecord(RecordByte buff, RecordPage page) throws Throwable{
		while(true){
			buff.putRawBytes(page.getPartialBytes());
			boolean endPart = page.endOfPartial();
			if(endPart){
				break;
			}
			page = getRecordPage(page.nextID, false);
		}
	}
	
	public void commit() throws Throwable {
		
		ArrayList<Page> tmp = new ArrayList<Page>(pageMap.values());
		int size = tmp.size();
		for(int i = 0; i < size; ++i){
			Page p = tmp.get(i);
			p.flush();
			pageMap.remove(p.getID());
		}
		
		for(Page p: pageMap.values()){
			p.flush();
		}
		
		//dataFile.close();
		freeList.save(fileName);
	}
}
