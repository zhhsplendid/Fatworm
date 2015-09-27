package filesys;

class RemainPage implements Comparable<RemainPage>{
	public long time;
	public int remainSize;
	public int pageID;
	
	public RemainPage(int size, int pid){
		remainSize = size;
		pageID = pid;
	}
	
	@Override
	public int compareTo(RemainPage o) {
		if(remainSize < o.remainSize){
			return -1;
		}
		else if(remainSize > o.remainSize){
			return 1;
		}
		else if(pageID < o.pageID){
			return -1;
		}
		else if(pageID > o.pageID){
			return 1;
		}
		return 0;
	}
	
	public boolean equals(Object o){
		if(o instanceof RemainPage){
			return pageID == ((RemainPage)o).pageID;
		}
		return false;
	}
}
