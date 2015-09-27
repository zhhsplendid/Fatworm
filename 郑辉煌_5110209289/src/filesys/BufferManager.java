package filesys;

import java.util.*;

import fatworm.driver.*;


import output.Debug;


public class BufferManager {
	
	
	public MyFile dataFile;
	public FreeList freeList;
	public Map<Integer, Page> pageMap = new TreeMap<Integer, Page>();
	public String fileName;
	
	public TreeSet<Page> LRUqueue = new TreeSet<Page>(new Comparator<Page>(){
		public int compare(Page p1, Page p2){
			return p1.getTime().compareTo(p2.getTime());
		}
	});
	
	public BufferManager(String fileName, int fileType) {
		dataFile = new MyFile(fileName, fileType);
		freeList = FreeList.load(fileName);
		freeList.save(fileName);
		this.fileName = fileName;
	}
	
	
	
	public void close() throws Throwable {
		LinkedList<Page> tmp = new LinkedList<Page>(pageMap.values());
		for(Page p: tmp){
			assert !p.isInTransaction();
			p.flush();
			pageMap.remove(p.getID());
		}
		
		for(Page p: pageMap.values()){
			p.flush();
		}
		
		dataFile.close();
		freeList.save(fileName);
	}
	
	public synchronized Integer newPage(){
		return freeList.poll();
	}
	
	public synchronized void markDelPage(Integer pageId){
		freeList.add(pageId);
		Page toMark = pageMap.get(pageId);
		if(toMark != null){
			toMark.deleteMark();
			LRUqueue.remove(toMark);
			pageMap.remove(pageId);
		}
	}
	
	public BTreePage getBTreePage(BKeyManager btree, int pageId, int type, boolean create) throws Throwable{
		Page tmp = manageGetPage(pageId);
		if(tmp != null) return (BTreePage)tmp;
		BTreePage newPage = new BTreePage(btree, dataFile, pageId, type, create);
		pageMap.put(pageId, newPage);
		LRUqueue.add(newPage);
		return newPage;
	}
	
	public RecordPage getRecordPage(int pageId, boolean create) throws Throwable{
		Page tmp = manageGetPage(pageId);
		if(tmp != null) return (RecordPage)tmp;
		RecordPage newPage = new RecordPage(this, dataFile, pageId, create);
		pageMap.put(pageId, newPage);
		LRUqueue.add(newPage);
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
		while(DatabaseEngine.getInstance().outOfMemory()){
			DatabaseEngine.getInstance().flushOtherManager(this);
		}
		return null;
	}
	
	public synchronized boolean flushOutOnePage() throws Throwable{
		synchronized(LRUqueue){
			if(LRUqueue.isEmpty()){
				return false;
			}
			Page victim = null;
			Iterator<Page> iter = LRUqueue.iterator();
	
			while(iter.hasNext()){
				victim = iter.next();
				if(!victim.isInTransaction()){
					break;
				}
			}
			iter.remove();
			if(victim.isInTransaction()){
				Debug.warn("flushing out a page still in transaction, will try to recover.");
			}
			victim.flush();
			pageMap.remove(victim.getID());
			return true;
		}
	}



	public Page getRawPage(int pageId, boolean create) throws Throwable {
		Page tmp = manageGetPage(pageId);
		if(tmp != null) return (Page)tmp;
		Page page = new Page(dataFile, pageId, create);
		LRUqueue.add(page);
		pageMap.put(pageId, page);
		return page;
	}



	public Page newRawPage(int size) throws Throwable {
		Integer ans = Page.newPoolPage(size);
		if(ans == null){
			ans = newPage();
			return getRawPage(ans, true);
		}
		else{
			Page page = getRawPage(ans, false);
			page.newEntry();
			return page;
		}
	}
	
	public Integer getNextPage(Integer pageID) throws Throwable {
		return getRecordPage(pageID, false).nextID;
	}

	public Integer getPrevPage(Integer pageID) throws Throwable {
		return getRecordPage(pageID, false).preID;
	}
	
	public List<Record> getRecords(Integer pageId, Schema schema) {
		if(pageId < 0){
			return new ArrayList<Record>();
		}
		ArrayList<Record> ans = new ArrayList<Record>();
		
		try {
			RecordPage page = getRecordPage(pageId, false);
			if(!page.isPartial()){
				return page.getRecords(schema);
			}
			RecordByte buff = new RecordByte();
			while(true){
				buff.putRawBytes(page.getPartialBytes());
				if(page.endOfPartial()){
					break;
				}
				page = getRecordPage(page.nextID, false);
			}
			ans.add(Record.fromByte(schema, buff.getBytes()));
			
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}
}
