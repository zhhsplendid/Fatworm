package filesys;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.lang3.ArrayUtils;

import output.Debug;

import fatworm.driver.Record;
import fatworm.driver.Schema;



//TODO arrayUtil can be change to Lib

public class RecordPage extends Page{
	
	private Integer recordCount;
	private List<Integer> offsetList;
	private List<Record> records;
	private byte[] partialBytes;
	private BufferManager manager;
	
	public RecordPage(BufferManager b, MyFile file, int pageId, boolean create) throws Throwable {
		recordCount = 0;
		lastTime = System.currentTimeMillis();
		dataFile = file;
		pageID = pageId;
		manager = b;
		offsetList = new ArrayList<Integer>();
		byte[] tmp = new byte[MyFile.RECORD_PAGE_SIZE];
		if(!create){
			dataFile.read(tmp, pageID);
			fromBytes(tmp);
		}
		else{
			records = new ArrayList<Record>();
			nextID = pageID;
			preID = pageID;
			dirty = true;
			buff = ByteBuffer.wrap(tmp);
			int length;
			if(offsetList.size() > 0){
				length = offsetList.get(offsetList.size() - 1);
			}
			else{
				length = 0;
			}
			partialBytes = new byte[length];
			buff.position(MyFile.RECORD_PAGE_SIZE - length);
			buff.get(partialBytes);
			ArrayUtils.reverse(partialBytes);
			records = new ArrayList<Record>();
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
		if(isPartial()){
			int length = buff.getInt();
			partialBytes = new byte[length];
			buff.get(partialBytes);
		}else{

			int length;
			if(offsetList.size() > 0){
				length = offsetList.get(offsetList.size() - 1);
			}
			else{
				length = 0;
			}
			partialBytes = new byte[length];
			buff.position((MyFile.RECORD_PAGE_SIZE - length));
			buff.get(partialBytes);
			ArrayUtils.reverse(partialBytes);
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
			
			if(!isPartial()){
				byte[] tmp = expandAndReverse(partialBytes);
				buff.position(MyFile.RECORD_PAGE_SIZE - tmp.length);
				buff.put(tmp);
			} else {
				buff.putInt(partialBytes.length);
				buff.put(partialBytes);
			}
		}else{
			int id = pageID;
			int curOffset = 0;
			int next = nextID;
			List<Integer> offsetBak = new ArrayList<Integer>(offsetList);
			List<Record> recordsBak = new ArrayList<Record>(records);
			int recordCountBak = recordCount;
			while(curOffset < recordCountBak){
				int fitCnt = 1;
				for(; fitCnt + curOffset < recordCountBak && canThisFit(offsetBak, curOffset, fitCnt); fitCnt++);
				fitCnt = Math.min(recordCountBak - curOffset, fitCnt);
				if(!canThisFit(offsetBak, curOffset, fitCnt))
					fitCnt--;
				Debug.warn("page "+ pageID + " separates with "+ (recordCountBak-curOffset) + " remaining, this id = " + id);
				if(curOffset == 0){
					recordCount = Math.max(1, fitCnt);
					offsetList = offsetList.subList(0, recordCount);
					records = records.subList(0, recordCount);
				}
				if(fitCnt >= 1){
					id = fitRecords(id, recordsBak.subList(curOffset, curOffset + fitCnt));
					curOffset += fitCnt;
				}else {
					int recordSize = offsetBak.get(curOffset) - (curOffset > 0 ? offsetBak.get(curOffset-1):0);
					id = addPartial(partialBytes, id, curOffset, recordSize);
					curOffset += 1;
				}
			}
			RecordPage rp = manager.getRecordPage(next, false);
			rp.beginTransaction();
			rp.dirty = rp.preID != id;
			rp.preID = id;
			rp.commit();
			rp = manager.getRecordPage(id, false);
			rp.beginTransaction();
			rp.dirty = rp.nextID!=next;
			rp.nextID = next;
			rp.commit();
			
		}
		return buff.array();
	}
	
	private Integer fitRecords(int id, List<Record> recordList) throws Throwable{
		int preId = id;
		id = manager.newPage();
		if(preId >= 0){
			RecordPage rp = manager.getRecordPage(preId, false);
			rp.beginTransaction();
			rp.dirty = (rp.nextID != id);
			rp.nextID = id;
			rp.commit();
		}
		RecordPage thisPage = manager.getRecordPage(id, true);
		thisPage.beginTransaction();
		thisPage.dirty = true;
		thisPage.preID = preId;
		int fitCnt = recordList.size();
		for(int j = 0; j < fitCnt; ++j){
			boolean flag = thisPage.tryAddRecord(recordList.get(j));
			assert flag;
		}
		thisPage.commit();
		return id;
	}
	
	private Integer fitRecord(Integer id, Record record) throws Throwable{
		int preId = id;
		id = manager.newPage();
		if(preId >= 0){//
			RecordPage rp = manager.getRecordPage(preId, false);
			rp.beginTransaction();
			rp.dirty = true;
			rp.nextID = id;
			rp.commit();
		}
		RecordPage nowPage = manager.getRecordPage(id, true);
		nowPage.beginTransaction();
		nowPage.dirty = true;
		nowPage.preID = preId;
		boolean flag = nowPage.tryAddRecord(record);
		assert flag;
		nowPage.commit();
		return id;
	}
	
	

	public boolean tryAddRecord(Record record) {
		parseRecord(record.schema);
		byte[] recordBytes = record.toByte();
		int length = recordBytes.length;
		
		if(length > maxFitSize()){
			return appendPartialRecord(recordBytes, record, true);
		}
		else if(partialBytes.length + length + countHeaderSize(recordCount + 1) > MyFile.RECORD_PAGE_SIZE){
			return appendPartialRecord(recordBytes, record, false);
		}
		updateRecord(record, recordCount);
		return true;
	}
	
	private boolean appendPartialRecord(byte[] recordBytes, Record record, boolean partition) {
		try{
			dirty = true;
			int next = nextID;
			int id;
			if(partition){
				id = addPartial(recordBytes, pageID, 0, recordBytes.length);
			}
			else{
				id = fitRecord(pageID, record);
			}
			RecordPage page = manager.getRecordPage(next, false);
			page.beginTransaction();
			page.preID = id;
			page.commit();
			page = manager.getRecordPage(id, false);
			page.beginTransaction();
			page.dirty = page.nextID != next;
			page.nextID = next;
			page.commit();
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
			if(preId != null){
				RecordPage page = manager.getRecordPage(preId, false);
				page.beginTransaction();
				page.dirty = true;
				page.nextID = pageId;
				page.commit();
			}
			RecordPage nowPage = manager.getRecordPage(pageId, true);
			nowPage.beginTransaction();
			nowPage.dirty = true;
			nowPage.preID = preId;
			nowPage.putPartial(recordBytes, startOffset + fitSize,
					maxFitSize(), fitSize + maxFitSize() >= length);
			fitSize += maxFitSize();
			nowPage.commit();
		}
		return 0;
	}

	private void putPartial(byte[] recordBytes, int start, int maxFitSize, boolean end) throws Throwable {
		beginTransaction();
		dirty = true;
		recordCount = 1;
		records = new ArrayList<Record>();
		int length = Math.min(maxFitSize, recordBytes.length - start);
		partialBytes = new byte[length];
		System.arraycopy(recordBytes, start, partialBytes, 0, length);
		offsetList = new ArrayList<Integer>();
		offsetList.add(end ? -2: -1);
		commit();
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
		for(int i = index + 1; i < offsetList.size(); ++i){
			offsetList.set(i, offsetList.get(i) + length);
		}
		++recordCount;
	}
	
	public void delRecord(Schema schema, int index){
		dirty = true;
		parseRecord(schema);
		records.remove(index);
		int length = offsetList.size() > 0 ? offsetList.get(offsetList.size()-1) : 0;
		int lastOffset = index > 0 ? offsetList.get(index-1):0;
		int lengthOfRecord = offsetList.get(index) - lastOffset;
		byte[] newbytes = new byte[length - lengthOfRecord];
		System.arraycopy(partialBytes, 0, newbytes, 0, lastOffset);
		System.arraycopy(partialBytes, offsetList.get(index), newbytes, 0, length - offsetList.get(index));
		partialBytes = newbytes;
		offsetList.remove(index);
		for(int i = index; i < offsetList.size(); ++i)
			offsetList.set(i, offsetList.get(i) - lengthOfRecord);
		--recordCount;
	}
	
	private void parseRecord(Schema schema){
		if(records != null){
			return;
		}
		
		records = new ArrayList<Record>();
		buff.position(headerSize());
		
		int lastOffset = 0;
		for(int i = 0; i < recordCount; ++i){
			int length = offsetList.get(i) - lastOffset;
			byte[] tmp = new byte[length];
			System.arraycopy(partialBytes, lastOffset, tmp, 0, length);
			records.add(Record.fromByte(schema, tmp));
			lastOffset = offsetList.get(i);
		}
	}

	private boolean canThisFit(List<Integer> offsetBak, int curOffset, int fitCnt) {
		return offsetBak.get(curOffset + fitCnt - 1) - (curOffset > 0 ? offsetBak.get(curOffset - 1):0) + getHeaderSize(fitCnt) <= MyFile.RECORD_PAGE_SIZE;
	}

	private int getHeaderSize(int cnt) {
		return (3 + cnt) * Integer.SIZE / Byte.SIZE;
	}

	private byte[] expandAndReverse(byte[] b) {
		byte[] ans = new byte[MyFile.RECORD_PAGE_SIZE - headerSize()];
		System.arraycopy(b, 0, ans, 0, b.length);
		ArrayUtils.reverse(ans);
		return ans;
	}
	
	public int headerSize(){
		return (3 + recordCount) * Integer.SIZE / Byte.SIZE; 
	}
	
	public boolean isPartial(){
		return recordCount == 1 && offsetList.get(0) == -1;
	}
	
	public boolean endOfPartial(){
		return recordCount == 1 && offsetList.get(0) == -2;
	}
	
	private static int maxFitSize(){
		int begin = 5 * Integer.SIZE / Byte.SIZE;
		// 3 for recordCount, preId, nextId, 1 for offsetList when only one element
		// 1 for ispartial
		return MyFile.RECORD_PAGE_SIZE - begin;
	}
	
	private int countHeaderSize(int cnt){
		return (3 + cnt) * Integer.SIZE / Byte.SIZE;
	}

	public List<Record> getRecords(Schema schema) {
		parseRecord(schema);
		return records;
	}
	
	public byte[] getPartialBytes(){
		return partialBytes;
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
			page.beginTransaction();
			page.nextID = nextID;
			page.dirty = true;
			page.commit();
			
			page = manager.getRecordPage(nextID, false);
			page.beginTransaction();
			page.preID = preID;
			page.dirty = true;
			page.commit();
			
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
