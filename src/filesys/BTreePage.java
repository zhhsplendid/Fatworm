package filesys;

import java.awt.Window.Type;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import output.Debug;


public class BTreePage extends Page{
	public long time;
	public static int easyErrCount = 0; // for debug;
	public static final int LEAF = 0;
	public static final int ROOT = 1;
	public static final int INTERNAL = 2;
	public static final int ROOT_LEAF = 3;
	
	private int nodeType;
	public int keyType;
	public int parentID;
	
	public ArrayList<Integer> children;
	public ArrayList<BKey> key;
	
	public BKeyManager btree;
	
	public int child_max;
	
	
	
	public BTreePage(BKeyManager btree, int pageId, int type, MyFile file,
			boolean create) throws Throwable {
		//super(pageId, file, create);
		lastTime = System.currentTimeMillis();
		keyType = type;
		pageID = pageId;
		dataFile = file;
		this.btree = btree;
		byte[] tmp = new byte[MyFile.RECORD_PAGE_SIZE];
		
		
		child_max = childMaxSize(keyType);
		if(create){
			buff = ByteBuffer.wrap(tmp);
			preID = nextID = parentID = -1;	
			key = new ArrayList<BKey>();
			children = new ArrayList<Integer>();
			children.add(-1);
			nodeType = ROOT_LEAF;
			dirty = true;
		}
		else{
			dataFile.read(tmp, pageID);
			children = new ArrayList<Integer>();
			key = new ArrayList<BKey>();
			fromBytes(tmp);
		}
	}
	
	private void countWrite(){
		++easyErrCount;
	}
	
	public void fromBytes(byte[] b) throws Throwable{
		buff = ByteBuffer.wrap(b);
		nodeType = buff.getInt();
		parentID = buff.getInt();
		countWrite();
		preID = buff.getInt();
		nextID = buff.getInt();
		
		int childCount = buff.getInt();
		//ByteBuffer buff = ByteBuffer.wrap(b);
		for(int i = 0; i < childCount; ++i){
			children.add(buff.getInt());
		}
		for(int i = 0; i < childCount - 1; ++i){
			
			if(keySize(keyType) != Integer.SIZE / Byte.SIZE){
				long number;
				number = buff.getLong();
				key.add(btree.getBKey(number, keyType));
			}
			else{
				int number;
				number = buff.getInt();
				key.add(btree.getBKey(number, keyType));
			}
		}
		//this.buff = buff;
	}
	
	private int keySize(int type) {
		switch(type){
		case java.sql.Types.INTEGER:
		case java.sql.Types.DECIMAL:
		case java.sql.Types.FLOAT:
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
			return 4;//Integer.SIZE / Byte.SIZE;
		case java.sql.Types.TIMESTAMP:
		case java.sql.Types.DATE:
			return 8;//Long.SIZE / Byte.SIZE;
		default:
			Debug.err("key size error");
		}
		return 4;
	}

	public byte[] toBytes() throws Throwable{
		buff.position(0);
		buff.putInt(nodeType);
		buff.putInt(parentID);
		countWrite();
		buff.putInt(preID);
		buff.putInt(nextID);
		
		
		int size = children.size();
		buff.putInt(size);
		for(int i = 0; i < size; ++i){
			buff.putInt(children.get(i));
		}
		size = key.size();
		for(int i = 0; i < size; ++i){
			buff.put(key.get(i).toByte());
		}
		byte[] ans = buff.array();
		return ans;
	}
	
	private int childMaxSize(int type) {
		switch(type){
		case java.sql.Types.INTEGER:
		case java.sql.Types.FLOAT:
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.CHAR:
		case java.sql.Types.DECIMAL:
		case java.sql.Types.VARCHAR:
			return (MyFile.BTREE_PAGE_SIZE - 16) >> 3;
		case java.sql.Types.TIMESTAMP:
		case java.sql.Types.DATE:
			return (MyFile.BTREE_PAGE_SIZE >> 10) * 85;
				/*
			switch(MyFile.BTREE_PAGE_SIZE){
			case 8192:
				return 680;
			case 4096:
				return 340;
			case 2048:
				return 168;
			case 1024:
				return 84;
			}*/
			default:
				Debug.err("err in section size");
		}
		return 340;
	}
	
	public boolean isLeaf(){
		return nodeType == LEAF || nodeType == ROOT_LEAF;
	}
	public boolean isRoot(){
		return nodeType == ROOT || nodeType == ROOT_LEAF;
	}
	public boolean isInternal(){
		return nodeType == INTERNAL;
	}
	
	public boolean notFull(){
		return children.size() < child_max;
	}
	public boolean hasNext(){
		return nextID != -1;
	}
	public boolean hasPre(){
		return preID != -1;
	}
	
	public int indexOf(BKey keyInput){
		countWrite();
		int index = Collections.binarySearch(key, keyInput, new Comparator<BKey>(){
			@Override
			public int compare(BKey b0, BKey b1) {
				return b0.compareTo(b1);
			}
		});
		if(index >= 0){
			return index;
		}
		else{
			return (-1) - index;
		}
	}
	
	@Override
	public int headerSize(){
		return 5 * Byte.SIZE;
	}
	
	
	public synchronized void addAtThisNode(Integer index, BKey k, int value){
		dirty = true;
		key.add(index, k);
		if(index + 1 > children.size()){
			System.out.println(index);
		}
		children.add(index + 1, value);
	}
	
	public synchronized void insert(BKey k, Integer index, int value) throws Throwable{
		dirty = true;
		if(notFull()){
			addAtThisNode(index, k, value);
		}
		else{
			countWrite();
			boolean isLeaf = isLeaf();
			boolean isRoot = isRoot();
			
			//writeBegin();
			countWrite();
			BTreePage newPage = btree.manager.getBTreePage(btree.manager.newPage(), keyType, btree, true);
			//newPage.writeBegin();
			newPage.parentID = parentID;
			newPage.preID = pageID;
			newPage.nextID = nextID;
			if(!isLeaf){
				nodeType = INTERNAL;
			}
			else{
				nodeType = LEAF;
			}
			
			newPage.nodeType = nodeType;
			
			boolean BTreehasNext = hasNext();
			if(BTreehasNext){
				//maybe change
				BTreePage nextPage = nextPage();
				//nextPage.writeBegin();
				nextPage.preID = newPage.getID();
				nextPage.dirty = true;
				nextPage.writeOver();
			}
			
			nextID = newPage.pageID;
			newPage.dirty = true;
			dirty = true;
			
			int mid = (child_max + 1) >> 1;
			int tmpIndex = indexOf(k);
			children.add(tmpIndex + 1, value);
			key.add(tmpIndex, k);
			
			countWrite();
			ArrayList<BKey> newkey1 = new ArrayList<BKey>();
			ArrayList<BKey> newkey2 = new ArrayList<BKey>();
			ArrayList<Integer> newChild1 = new ArrayList<Integer> ();
			ArrayList<Integer> newChild2 = new ArrayList<Integer> ();
			
			BKey toParent = key.get(mid);
			if(! isLeaf){
				for(int i = 0; i < mid - 1; ++i){
					newkey1.add(key.get(i));
				}
				for(int i = mid; i < key.size(); ++i){
					newkey2.add(key.get(i));
				}
			}
			else{
				for(int i = 0; i < mid; ++i){
					newkey1.add(key.get(i));
				}
				for(int i = mid; i < key.size(); ++i){
					newkey2.add(key.get(i));
				}
			}
			key = newkey1;
			newPage.key = newkey2;
			
			
			
			if(!isLeaf){
				for(int i = 0; i < mid; ++i){
					newChild1.add(children.get(i));
				}
				int size = children.size();
				for(int i = mid; i < size; ++i){
					Integer cpid = children.get(i);
					BTreePage cp = getPage(cpid);
					//cp.writeBegin();
					cp.parentID = newPage.pageID;
					cp.writeOver();
					newChild2.add(cpid);
				}
			}else {
				int bound = mid + 1;
				for(int i = 0; i < bound; ++i){
					newChild1.add(children.get(i));
				}
				newChild2.add(-1);
				int size = children.size();
				for(int i = bound; i < size; ++i){
					newChild2.add(children.get(i));
				}
				
			}
			children = newChild1;
			newPage.children = newChild2;
			
			if(! isRoot){
				writeOver();
				countWrite();
				newPage.writeOver();
				int changeIndex = -1;
				BTreePage father = parentPage();
				int size = father.children.size();
				for(int i = 0; i < size; ++i){
					if(pageID == father.children.get(i)){
						changeIndex = i;
						break;
					}
				}
				countWrite();
				//father.writeBegin();
				father.insert(toParent, changeIndex, newPage.pageID);
			}
			else{
				countWrite();
				BTreePage newRoot = btree.manager.getBTreePage(btree.manager.newPage(), keyType, btree, true);
				parentID = newRoot.pageID;
				writeOver();
				newPage.parentID = newRoot.pageID;
				newPage.writeOver();
				//newRoot.writeBegin();
				countWrite();
				newRoot.children = new ArrayList<Integer>();
				newRoot.children.add(pageID);
				newRoot.children.add(newPage.pageID);
				newRoot.key.add(toParent);
				newRoot.nodeType = ROOT;
				countWrite();
				btree.table.connectNewRoot(btree.root.getID(), newRoot.getID());
				btree.root = newRoot;
				newRoot.dirty = true;
				newRoot.writeOver();
			}
		}
	}
	
	public synchronized void remove(Integer index) throws Throwable{
		//writeBegin();
		children.remove(index);
		key.remove(index);
		dirty = true;
		writeOver();
	}
	
	private BTreePage getPage(Integer pid) throws Throwable {
		return btree.manager.getBTreePage(pid, keyType, btree, false);
	}
	

	
	public BTreePage nextPage() throws Throwable{
		return btree.manager.getBTreePage(nextID, keyType, btree, false);
	}
	
	public BTreePage prePage() throws Throwable{
		return btree.manager.getBTreePage(preID, keyType, btree, false);
	}
	public BTreePage parentPage() throws Throwable{
		return btree.manager.getBTreePage(parentID, keyType, btree, false);
	}
	
	public synchronized BTreeCursor firstCursor() throws Throwable {
		if(isLeaf()){
			return new BTreeCursor(0);
		}
		else{
			return getPage(children.get(0)).firstCursor();
		}
	}
	
	public synchronized BTreeCursor lastCursor() throws Throwable {
		if(isLeaf()){
			return new BTreeCursor(key.size());
		}
		else{
			return getPage(children.get(key.size())).lastCursor();
		}
	}
	
	public BTreeCursor cursorOfKey(BKey k) throws Throwable{
		int index = indexOf(k);
		if(isLeaf()){
			return new BTreeCursor(index);
		}
		else{
			return getPage(children.get(index)).cursorOfKey(k);
		}
	}
	
	public void drop() throws Throwable{
		if(! isLeaf()){
			int size = children.size();
			for(int i = 0; i < size; ++i){
				getPage(children.get(i)).drop();
			}
		}
		int size = key.size();
		for(int i = 0; i < size; ++i){
			key.get(i).remove();
		}
		btree.manager.markDelPage(pageID);
	}
	
	public class BTreeCursor {
		public int index;
		
		public BTreeCursor(int idx){
			index = idx;
		}
		
		public boolean valid(){
			return index < key.size() && index >= 0;
		}
		
		public BKey getKey(){
			return key.get(index);
		}
		
		public int getValue() {
			return children.get(index + 1);
		}
		
		public boolean hasNext(){
			int maxIndex = key.size() - 1;
			return index < maxIndex || BTreePage.this.hasNext();
		}
		
		public BTreeCursor next() throws Throwable{
			int maxIndex = key.size() - 1;
			if(index < maxIndex){
				return new BTreeCursor(index + 1);
			}
			else{
				BTreePage npage = BTreePage.this.nextPage();
				return npage.firstCursor();
			}
		}
		
		public boolean hasPrev(){
			return index > 0 || BTreePage.this.hasPre();
		}
		
		public BTreeCursor prev() throws Throwable{
			if(index <= 0){
				BTreeCursor cur = BTreePage.this.prePage().lastCursor();
				return cur.prev();
			}
			else {
					
				//**
				return new BTreeCursor(index - 1);
			}
		}
		
		public void insert(BKey bkey, int value) throws Throwable{
			//assert valid() || index == key.size();
			//BTreePage.this.writeBegin();
			countWrite();
			BTreePage.this.insert(bkey, index, value);
			BTreePage.this.writeOver();
		}
		
		public void remove() throws Throwable{
			//assert valid();
			//BTreePage.this.writeBegin();
			countWrite();
			BTreePage.this.remove(index);
			BTreePage.this.writeOver();
		}
		
		
		
		public int getPageId() {
			return BTreePage.this.getID();
		}

		public BTreeCursor adjust() throws Throwable {
			if(valid()){
				countWrite();
				return this;
			}
			if(index == key.size() && BTreePage.this.hasNext()){
				BTreePage tmp = BTreePage.this.nextPage();
				return tmp.firstCursor();
			}
			return null;
		}

		public BTreeCursor adjustLeft() {
			if(valid()){
				countWrite();
				return this;
			}
			else if(index > 0 && index == key.size()){
				return new BTreeCursor(index - 1);
			}
			return null;
		}
		@Override
		public BTreeCursor clone(){
			return new BTreeCursor(index);
		}
		@Override
		public boolean equals(Object o){
			if(o instanceof BTreeCursor){
				return ((BTreeCursor) o).index == index && (getPageId() == ((BTreeCursor) o).getPageId());
			}
			return false;
		}
	}
	
}
