package filesys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;


import output.Debug;


public class BTreePage extends Page{
	public static final int ROOT = 1;
	public static final int INTERNAL = 2;
	public static final int LEAF = 3;
	public static final int ROOT_LEAF = 4;
	
	private int nodeType;
	public int keyType;
	public int parentID;
	
	public ArrayList<Integer> children;
	public ArrayList<BKey> key;
	
	public BKeyManager btree;
	
	public int child_max;
	
	public BTreePage(BKeyManager btree, MyFile file, int pageId, int type,
			boolean create) throws Throwable {
		lastTime = System.currentTimeMillis();
		dataFile = file;
		pageID = pageId;
		byte[] tmp = new byte[MyFile.RECORD_PAGE_SIZE];
		keyType = type;
		btree = btree;
		
		child_max = childMaxSize(keyType);
		if(! create){
			dataFile.read(tmp, pageID);
			children = new ArrayList<Integer>();
			key = new ArrayList<BKey>();
			fromBytes(tmp);
		}
		else{
			nextID = -1;
			preID = -1;
			parentID = -1;
			children = new ArrayList<Integer>();
			key = new ArrayList<BKey>();
			dirty = true;
			children.add(-1);
			nodeType = ROOT_LEAF;
			buff = ByteBuffer.wrap(tmp);
		}
	}
	
	public void fromBytes(byte[] b) throws Throwable{
		ByteBuffer buff = ByteBuffer.wrap(b);
		parentID = buff.getInt();
		preID = buff.getInt();
		nextID = buff.getInt();
		int childCount = buff.getInt();
		for(int i = 0; i < childCount; ++i){
			children.add(buff.getInt());
		}
		for(int i = 0; i < childCount; ++i){
			if(keySize(keyType) == Integer.SIZE / Byte.SIZE){
				key.add(btree.getBKey(buff.getInt(), keyType));
			}
			else{
				key.add(btree.getBKey(buff.getLong(), keyType));
			}
		}
		this.buff = buff;
	}
	
	private int keySize(int type) {
		switch(type){
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.INTEGER:
		case java.sql.Types.FLOAT:
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
		case java.sql.Types.DECIMAL:
			return Integer.SIZE / Byte.SIZE;
		case java.sql.Types.DATE:
		case java.sql.Types.TIMESTAMP:
			return Long.SIZE / Byte.SIZE;
		default:
			Debug.err("key size error");
		}
		return 4;
	}

	public byte[] toBytes() throws Throwable{
		buff.position(0);
		buff.putInt(parentID);
		buff.putInt(preID);
		buff.putInt(nextID);
		buff.putInt(nodeType);
		buff.putInt(children.size());
		for(int i = 0; i < children.size(); ++i){
			buff.putInt(children.get(i));
		}
		for(int i = 0; i < key.size(); ++i){
			buff.put(key.get(i).toByte());
		}
		return buff.array();
	}
	
	private int childMaxSize(int type) {
		switch(type){
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.INTEGER:
		case java.sql.Types.FLOAT:
			switch(MyFile.BTREE_PAGE_SIZE){
			case 1024:
				return 126;
			case 2048:
				return 254;
			case 4096:
				return 510;
			case 8192:
				return 1022;
			}
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
		case java.sql.Types.DECIMAL:
			
			switch(MyFile.BTREE_PAGE_SIZE){
			case 1024:
				return 126;
			case 2048:
				return 254;
			case 4096:
				return 510;
			case 8192:
				return 1022;
			}
		case java.sql.Types.DATE:
		case java.sql.Types.TIMESTAMP:
			switch(MyFile.BTREE_PAGE_SIZE){
			case 1024:
				return 84;
			case 2048:
				return 168;
			case 4096:
				return 340;
			case 8192:
				return 680;
			}
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
	public boolean isFull(){
		assert child_max >= children.size();
		return child_max == children.size();
	}
	public boolean hasNext(){
		return nextID != -1;
	}
	public boolean hasPre(){
		return preID != -1;
	}
	
	public int indexOf(BKey k){
		int idx = Collections.binarySearch(key, k, new Comparator<BKey>(){
			@Override
			public int compare(BKey arg0, BKey arg1) {
				return arg0.compareTo(arg1);
			}
		});
		return idx < 0 ? (-1)-idx: idx;
	}
	
	@Override
	public int headerSize(){
		return 5 * Byte.SIZE;
	}
	
	public synchronized void addAtThisNode(Integer index, BKey k, int value){
		dirty = true;
		key.add(index, k);
		children.add(index + 1, value);
	}
	
	public synchronized void insert(Integer index, BKey k, int value) throws Throwable{
		dirty = true;
		if(!isFull()){
			addAtThisNode(index, k, value);
		}
		else{
			boolean isRoot = isRoot();
			boolean isLeaf = isLeaf();
			
			beginTransaction();
			BTreePage newPage = btree.manager.getBTreePage(btree, btree.manager.newPage(), keyType, true);
			newPage.beginTransaction();
			newPage.preID = pageID;
			newPage.nextID = nextID;
			newPage.parentID = parentID;
			nodeType = isLeaf ? LEAF : INTERNAL;
			newPage.nodeType = nodeType;
			
			if(hasNext()){
				//maybe change
				BTreePage nextPage = nextPage();
				nextPage.beginTransaction();
				nextPage.preID = newPage.getID();
				nextPage.dirty = true;
				nextPage.commit();
			}
			
			nextID = newPage.pageID;
			newPage.dirty = true;
			dirty = true;
			
			int mid = (child_max + 1) >> 1;
			int tmpIndex = indexOf(k);
			key.add(tmpIndex, k);
			children.add(tmpIndex + 1, value);
			
			ArrayList<BKey> newkey1 = new ArrayList<BKey>();
			ArrayList<BKey> newkey2 = new ArrayList<BKey>();
			BKey toParent = key.get(mid);
			if(isLeaf){
				for(int i = 0; i < mid; ++i){
					newkey1.add(key.get(i));
				}
				for(int i = mid; i < key.size(); ++i){
					newkey2.add(key.get(i));
				}
			}
			else{
				for(int i = 0; i < mid-1; ++i)
					newkey1.add(key.get(i));
				for(int i = mid; i < key.size(); ++i)
					newkey2.add(key.get(i));
			}
			key = newkey1;
			newPage.key = newkey2;
			
			ArrayList<Integer> newChild1 = new ArrayList<Integer> ();
			ArrayList<Integer> newChild2 = new ArrayList<Integer> ();
			if(isLeaf){
				for(int i = 0; i < mid+1; ++i)
					newChild1.add(children.get(i));
				newChild2.add(-1);
				for(int i = mid+1; i<children.size(); ++i){
					newChild2.add(children.get(i));
				}
			}else {
				for(int i=0;i<mid;i++)
					newChild1.add(children.get(i));
				for(int i=mid;i<children.size();i++){
					Integer cpid = children.get(i);
					newChild2.add(cpid);
					BTreePage cp = getPage(cpid);
					cp.beginTransaction();
					cp.parentID = newPage.pageID;
					cp.commit();
				}
			}
			children = newChild1;
			newPage.children = newChild2;
			
			if(isRoot){
				BTreePage newRoot = btree.manager.getBTreePage(btree, btree.manager.newPage(), keyType, true);
				parentID = newRoot.pageID;
				commit();
				newPage.parentID = newRoot.pageID;
				newPage.commit();
				newRoot.beginTransaction();
				newRoot.nodeType = ROOT;
				newRoot.key.add(toParent);
				newRoot.children = new ArrayList<Integer>();
				newRoot.children.add(pageID);
				newRoot.children.add(newPage.pageID);
//				DBEngine.getInstance().announceBTreeNewRoot(btree.root.getID(), newRoot.getID());
				btree.table.announceNewRoot(btree.root.getID(), newRoot.getID());
//				Util.warn("BTree changing root!oldRoot="+btree.root.getID()+", newRoot="+newRoot.getID());
				btree.root = newRoot;
				newRoot.dirty = true;
				newRoot.commit();
			}
			else{
				commit();
				newPage.commit();
				int changeIndex = -1;
				BTreePage father = parentPage();
				for(int i = 0; i < father.children.size(); ++i){
					if(pageID == father.children.get(i)){
						changeIndex = i;
						break;
					}
				}
				father.beginTransaction();
				father.insert(changeIndex, toParent, newPage.pageID);
			}
		}
	}
	
	public synchronized void remove(Integer index) throws Throwable{
		beginTransaction();
		dirty = true;
		key.remove(index);
		children.remove(index);
		commit();
	}
	
	private BTreePage getPage(Integer pid) throws Throwable {
		return btree.manager.getBTreePage(btree, pid, keyType, false);
	}
	

	
	public BTreePage nextPage() throws Throwable{
		return btree.manager.getBTreePage(btree, nextID, keyType, false);
	}
	
	public BTreePage prePage() throws Throwable{
		return btree.manager.getBTreePage(btree, preID, keyType, false);
	}
	public BTreePage parentPage() throws Throwable{
		return btree.manager.getBTreePage(btree, parentID, keyType, false);
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
			for(int i = 0; i < children.size(); ++i){
				getPage(children.get(i)).drop();
			}
		}
		for(int i = 0; i < key.size(); ++i){
			key.get(i).remove();
		}
		btree.manager.markDelPage(pageID);
	}
	
	public class BTreeCursor {
		public final int index;
		
		public BTreeCursor(int idx){
			index = idx;
		}
		
		public boolean valid(){
			return index >= 0 && index < key.size();
		}
		
		public BKey getKey(){
			assert valid();
			return key.get(index);
		}
		
		public int getValue() {
			assert valid();
			return children.get(index + 1);
		}
		
		public boolean hasNext(){
			return index + 1 < key.size() || BTreePage.this.hasNext();
		}
		
		public BTreeCursor next() throws Throwable{
			assert valid();
			if(index + 1 < key.size())
				return new BTreeCursor(index+1);
			else return BTreePage.this.nextPage().firstCursor();
		}
		
		public boolean hasPrev(){
			return index > 0 || BTreePage.this.hasPre();
		}
		
		public BTreeCursor prev() throws Throwable{
//			assert valid();
			if(index > 0){
				return new BTreeCursor(index - 1);
			}
			else {
				return BTreePage.this.prePage().lastCursor().prev();	
				//**
			}
		}
		
		public void insert(BKey bkey, int value) throws Throwable{
			assert valid() || index == key.size();
			BTreePage.this.beginTransaction();
			BTreePage.this.insert(index, bkey, value);
			BTreePage.this.commit();
		}
		
		public void remove() throws Throwable{
			assert valid();
			BTreePage.this.beginTransaction();
			BTreePage.this.remove(index);
			BTreePage.this.commit();
		}
		
		
		
		public int getPageId() {
			return BTreePage.this.getID();
		}

		public BTreeCursor adjust() throws Throwable {
			if(valid()){
				return this;
			}
			if(index == key.size() && BTreePage.this.hasNext()){
				return BTreePage.this.nextPage().firstCursor();
			}
			return null;
		}

		public BTreeCursor adjustLeft() {
			if(valid()){
				return this;
			}
			if(index == key.size() && index > 0){
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
				return (getPageId() == ((BTreeCursor) o).getPageId()) && ((BTreeCursor) o).index == index;
			}
			return false;
		}
	}
	
}
