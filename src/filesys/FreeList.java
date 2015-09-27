package filesys;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedList;

import output.Debug;

public class FreeList implements Serializable {
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = 4812627806441915352L;
	public static final String FREELIST_SUFFIX = ".free";
	
	public LinkedList<Integer> freeList;
	public int nextPageNumber;
	//private static FreeList instance;
	
	private FreeList(){
		freeList = new LinkedList<Integer>();
		nextPageNumber = 0;
	}
	
	/*
	public FreeList getInstance(){
		if(instance == null){
			instance = new FreeList();
		}
		return instance;
	}*/
	
	public static FreeList load(String prefix){
		String freeName = prefix + FREELIST_SUFFIX;
		FreeList ret;
		try {
			ObjectInputStream fin = new ObjectInputStream(new FileInputStream(freeName));
			ret = (FreeList)fin.readObject();
			fin.close();
		} catch (IOException e) {
			ret = new FreeList();
		} catch (ClassNotFoundException e){
			ret = new FreeList();
		}
		return ret;
	}
	
	public void save(String prefix){
		String freeName = prefix + FREELIST_SUFFIX;
		try{
			ObjectOutputStream fout = new ObjectOutputStream(new FileOutputStream(freeName));
			fout.writeObject(this);
			fout.close();
		} catch (IOException e){
			e.printStackTrace();
			Debug.warn("freelist not write into file");
		}
	}
	
	public synchronized void add(int pid){
		freeList.add(pid);
	}
	
	public synchronized int getOnePage(){
		if(freeList.isEmpty()){
			return nextPageNumber++;
		}
		else{
			return freeList.poll();
		}
	}
}
