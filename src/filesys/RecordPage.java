package filesys;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.lang3.ArrayUtils;

import fatworm.driver.Record;
import fatworm.driver.Schema;



//TODO arrayUtil can be change to Lib

public class RecordPage extends Page{
	public long time;
	private Integer recordCount;
	private List<Integer> offsetList;
	private List<Record> records;
	private byte[] partialBytes;
	private BufferManager manager;
	
	public RecordPage(BufferManager b, int pageId, MyFile file, boolean create) throws Throwable {
		recordCount = 0;
		lastTime = System.currentTimeMillis();
		dataFile = file;
		pageID = pageId;
		manager = b;
		offsetList = new ArrayList<Integer>();
		byte[] tmp = new byte[MyFile.RECORD_PAGE_SIZE];
		if(create){
			records = new ArrayList<Record>();
			preID = pageID;
			nextID = pageID;
			dirty = true;
			buff = ByteBuffer.wrap(tmp);
			int length;
			if(offsetList.size() <= 0){
				length = 0;
			}
			else{
				length = offsetList.get(offsetList.size() - 1);
			}
			partialBytes = new byte[length];
			buff.position(MyFile.RECORD_PAGE_SIZE - length);
			buff.get(partialBytes);
			ArrayUtils.reverse(partialBytes);
			records = new ArrayList<Record>();
		}
		else{
			dataFile.read(tmp, pageID);
			fromBytes(tmp);
		}
	}
	
	public void fromBytes(byte[] b) throws Throwable{
		ByteBuffer buff = ByteBuffer.wrap(b);
		recordCount = buff.getInt();
		preID = buff.getInt();
		nextID = buff.getInt();
		
		for(int i = 0; i < recordCount; ++i){
			offsetList.add(buff.getInt());
		}
		if(! isPartial()){
			int length;
			if(offsetList.size() <= 0){
				length = 0;
			}
			else{
				length = offsetList.get(offsetList.size() - 1);
			}
			partialBytes = new byte[length];
			buff.position((MyFile.RECORD_PAGE_SIZE - length));
			buff.get(partialBytes);
			ArrayUtils.reverse(partialBytes);
		}else{
			int length = buff.getInt();
			partialBytes = new byte[length];
			buff.get(partialBytes);
		}
		this.buff = buff;
	}
	
	
	public byte[] toBytes() throws Throwable {
		if(partialBytes.length + headerSize() <= MyFile.RECORD_PAGE_SIZE){
			dirty = true;
			buff.position(0);
			buff.putInt(recordCount);
			buff.putInt(preID);
			buff.putInt(nextID);
			for(int i = 0; i < recordCount; ++i){
				buff.putInt(offsetList.get(i));
			}
			
			if(isPartial()){
				buff.putInt(partialBytes.length);
				buff.put(partialBytes);
			} 
			else {
				byte[] tmp = expandAndReverse(partialBytes);
				buff.position(MyFile.RECORD_PAGE_SIZE - tmp.length);
				buff.put(tmp);
			}
		}
		else{
			ArrayList<Integer> offsetBak = new ArrayList<Integer>(offsetList);
			ArrayList<Record> recordsBak = new ArrayList<Record>(records);
			int curOffset = 0;
			int recordCountBak = recordCount;
			int id = pageID;
			int next = nextID;
			while(curOffset < recordCountBak){
				int fitCnt = 1;
				while(fitCnt + curOffset < recordCountBak  && canThisFit(offsetBak, curOffset, fitCnt)){
					++fitCnt;
				}
				fitCnt = Math.min(recordCountBak - curOffset, fitCnt);
				if(!canThisFit(offsetBak, curOffset, fitCnt)){
					--fitCnt;
				}
				
				if(curOffset == 0){
					recordCount = Math.max(1, fitCnt);
					records = records.subList(0, recordCount);
					offsetList = offsetList.subList(0, recordCount);
				}
				if(fitCnt < 1){
					int recordSize;
					if(curOffset <= 0){
						recordSize = offsetBak.get(curOffset);
					}
					else{
						recordSize = offsetBak.get(curOffset) - offsetBak.get(curOffset - 1);
					}
					id = addPartial(partialBytes, id, curOffset, recordSize);
					curOffset += 1;
				}else {
					id = addRecordsWithId(id, recordsBak.subList(curOffset, curOffset + fitCnt));
					curOffset += fitCnt;
				}
			}
			RecordPage page = manager.getRecordPage(next, false);
			//page.writeBegin();
			page.dirty = page.preID != id;
			page.preID = id;
			page.writeOver();
			
			page = manager.getRecordPage(id, false);
			//page.writeBegin();
			page.dirty = page.nextID != next;
			page.nextID = next;
			page.writeOver();
			
		}
		return buff.array();
	}
	
	private Integer addRecordsWithId(int id, List<Record> recordList) throws Throwable{
		int preId = id;
		id = manager.newPage();
		boolean correctId = preId >= 0;
		if(correctId){
			RecordPage page = manager.getRecordPage(preId, false);
			//page.writeBegin();
			page.dirty = (page.nextID != id);
			page.nextID = id;
			page.writeOver();
		}
		RecordPage thisPage = manager.getRecordPage(id, true);
		//thisPage.writeBegin();
		thisPage.dirty = true;
		thisPage.preID = preId;
		int fitCnt = recordList.size();
		for(int j = 0; j < fitCnt; ++j){
			thisPage.addRecord(recordList.get(j));
			//boolean flag = thisPage.addRecord(recordList.get(j));
			//assert flag;
		}
		thisPage.writeOver();
		return id;
	}
	
	private Integer addRecordWithId(Integer id, Record record) throws Throwable{
		int preId = id;
		id = manager.newPage();
		boolean correctId = preId >= 0;
		if(correctId){//
			RecordPage rp = manager.getRecordPage(preId, false);
			//rp.writeBegin();
			rp.dirty = true;
			rp.nextID = id;
			rp.writeOver();
		}
		RecordPage nowPage = manager.getRecordPage(id, true);
		//nowPage.writeBegin();
		nowPage.dirty = true;
		nowPage.preID = preId;
		nowPage.addRecord(record);
		//boolean flag = nowPage.addRecord(record);
		//assert flag;
		nowPage.writeOver();
		return id;
	}
	
	

	public boolean addRecord(Record record) {
		parseRecord(record.schema);
		byte[] recordBytes = record.toByte();
		int length = recordBytes.length;
		
		if(length > MyFile.RECORD_PAGE_SIZE - 5 * Integer.SIZE / Byte.SIZE){ // > maxFileSize()
			boolean ans = addPartRecord(recordBytes, record, true);
			return ans;
		}
		else if(partialBytes.length + length + countHeaderSize(recordCount + 1) > MyFile.RECORD_PAGE_SIZE){
			boolean ans = addPartRecord(recordBytes, record, false);
			return ans;
		}
		updateRecord(record, recordCount);
		return true;
	}
	
	private boolean addPartRecord(byte[] recordBytes, Record record, boolean partition) {
		try{
			dirty = true;
			int next = nextID;
			int id;
			if(partition){
				id = addPartial(recordBytes, pageID, 0, recordBytes.length);
			}
			else{
				id = addRecordWithId(pageID, record);
			}
			RecordPage page = manager.getRecordPage(next, false);
			//page.writeBegin();
			page.preID = id;
			page.writeOver();
			page = manager.getRecordPage(id, false);
			//page.writeBegin();
			page.dirty = page.nextID != next;
			page.nextID = next;
			page.writeOver();
			return true;
			
		} catch(Throwable e){
			e.printStackTrace();
		}
		return false;
	}

	private int addPartial(byte[] recordBytes, Integer pageId, int startOffset, int length) 
			throws Throwable{
	
		int fitSize = 0;
		Integer preId = null;
		while(fitSize < length){
			preId = pageId;
			pageId = manager.newPage();
			boolean notNull = preId != null;
			if(notNull){
				RecordPage page = manager.getRecordPage(preId, false);
				//page.writeBegin();
				page.dirty = true;
				page.nextID = pageId;
				page.writeOver();
			}
			RecordPage nowPage = manager.getRecordPage(pageId, true);
			//nowPage.writeBegin();
			nowPage.dirty = true;
			nowPage.preID = preId;
			boolean over = fitSize + maxFitSize() >= length;
			nowPage.putPartial(recordBytes, startOffset + fitSize, maxFitSize(), over);
			fitSize += maxFitSize();
			nowPage.writeOver();
		}
		return 0;
	}

	private void putPartial(byte[] recordBytes, int start, int maxFitSize, boolean end) throws Throwable {
		//writeBegin();
		dirty = true;
		recordCount = 1;
		records = new ArrayList<Record>();
		offsetList = new ArrayList<Integer>();
		int length = Math.min(maxFitSize, recordBytes.length - start);
		partialBytes = new byte[length];
		System.arraycopy(recordBytes, start, partialBytes, 0, length);
		
		if(!end){
			offsetList.add(-1);
		}
		else{
			offsetList.add(-2);
		}
		writeOver();
	}

	/**
	 * this method is used for update record into bytes
	 * @param record
	 * @param index
	 */
	
	public void updateRecord(Record record, int index) {
		dirty = true;
		parseRecord(record.schema);
		records.add(index, record);
		byte[] recordBytes = record.toByte();
		
		int totalLength = 0;
		
		if(! offsetList.isEmpty()){
			totalLength = offsetList.get(offsetList.size() - 1);
		}
		int length = recordBytes.length;
		int lastOffset;
		if(index <= 0){
			lastOffset = 0;
		}
		else{
			lastOffset = offsetList.get(index - 1);
		}
		byte[] newBytes = new byte[totalLength + length];
		
		System.arraycopy(partialBytes, 0, newBytes, 0, lastOffset);
		System.arraycopy(recordBytes, 0, newBytes, lastOffset, length);
		System.arraycopy(partialBytes, lastOffset, newBytes, lastOffset + length, totalLength - lastOffset);
		
		//byte[] newByte = new byte[totalLength + length];
		partialBytes = newBytes;
		offsetList.add(index, lastOffset + length);
		int size = offsetList.size();
		for(int i = index + 1; i < size; ++i){
			offsetList.set(i, offsetList.get(i) + length);
		}
		++recordCount;
	}
	
	public void delRecord(Schema schema, int index){
		dirty = true;
		parseRecord(schema);
		records.remove(index);
		int length = 0, lastOffset = 0;
		if(index > 0){
			lastOffset = offsetList.get(index-1);
		}
		if(offsetList.size() > 0){
			length = offsetList.get(offsetList.size() - 1);
		}
		int lengthOfRecord = offsetList.get(index) - lastOffset;
		byte[] newbytes = new byte[length - lengthOfRecord];
		System.arraycopy(partialBytes, 0, newbytes, 0, lastOffset);
		System.arraycopy(partialBytes, offsetList.get(index), newbytes, lastOffset, length - offsetList.get(index));
		partialBytes = newbytes;
		offsetList.remove(index);
		int size = offsetList.size();
		for(int i = index; i < size; ++i){
			offsetList.set(i, offsetList.get(i) - lengthOfRecord);
		}
		--recordCount;
	}
	
	private void parseRecord(Schema schema){
		if(records == null){
			records = new ArrayList<Record>();
			buff.position(headerSize());
			
			int lastOffset = 0;
			for(int i = 0; i < recordCount; ++i){
				int length = offsetList.get(i) - lastOffset;
				byte[] tmp = new byte[length];
				System.arraycopy(partialBytes, lastOffset, tmp, 0, length);
				records.add(Record.fromByte(tmp, schema));
				lastOffset = offsetList.get(i);
			}
		}
	}

	private boolean canThisFit(List<Integer> offsetBak, int curOffset, int fitCnt) {
		int tmp2 = (curOffset > 0 ? offsetBak.get(curOffset - 1):0);
		int tmp1 = offsetBak.get(curOffset + fitCnt - 1);
		return tmp1 - tmp2 + getHeaderSize(fitCnt) <= MyFile.RECORD_PAGE_SIZE;
	}

	private int getHeaderSize(int cnt) {
		//return (3 + cnt) * Integer.SIZE / Byte.SIZE;
		return (3 + cnt) << 2;
	}

	private byte[] expandAndReverse(byte[] b) {
		int len = MyFile.RECORD_PAGE_SIZE - headerSize();
		byte[] ans = new byte[len];
		System.arraycopy(b, 0, ans, 0, b.length);
		ArrayUtils.reverse(ans);
		return ans;
	}
	
	public int headerSize(){
		return (3 + recordCount) * Integer.SIZE / Byte.SIZE; 
	}
	
	public boolean isPartial(){
		boolean ans = recordCount == 1 && offsetList.get(0) == -1;
		return ans;
	}
	
	public boolean endOfPartial(){
		boolean ans = recordCount == 1 && offsetList.get(0) == -2;
		return ans;
	}
	
	private static int maxFitSize(){
		//int begin = 5 * Integer.SIZE / Byte.SIZE;
		//int begin = 20;
		// 3 for recordCount, preId, nextId, 1 for offsetList when only one element
		// 1 for ispartial
		return MyFile.RECORD_PAGE_SIZE - 20;
	}
	
	private int countHeaderSize(int x){
		return (3 + x) << 2;
		//return Integer.SIZE / Byte.SIZE * (3 + x);
	}

	public byte[] getPartialBytes(){
		return partialBytes;
	}

	public List<Record> getRecords(Schema schema) {
		parseRecord(schema);
		return records;
	}
	
	public boolean canDrop() {
		return nextID != pageID && recordCount <= 0;
	}
	
	public boolean drop(){
		if(! canDrop()){
			return false;
		}
		try{
			RecordPage page = manager.getRecordPage(preID, false);
			//page.writeBegin();
			page.nextID = nextID;
			page.dirty = true;
			page.writeOver();
			
			page = manager.getRecordPage(nextID, false);
			//page.writeBegin();
			page.preID = preID;
			page.dirty = true;
			page.writeOver();
			
			records = null;
			partialBytes = null;
			offsetList = null;
			recordCount = null;
			
			manager.markDelPage(pageID);
		} catch(Throwable e){
			e.printStackTrace();
			return false;
		}
		return true;
	}
}
