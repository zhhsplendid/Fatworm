package filesys;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;

import output.Debug;

public class MyFile { 
	
	public static final int RECORD_PAGE_SIZE = 8 * 1024; //8KB
	public static final int BTREE_PAGE_SIZE = 4 * 1024;  //4KB
	
	public static final int RECORD_FILE = 0;
	public static final int BTREE_FILE = 1;
	
	int type; //RECORD_FILE or BTREE_FILE
	
	RandomAccessFile file;
	ByteBuffer buff;
	FileChannel fc;
	
	public MyFile(String fileName, int t){
		type = t;
		load(fileName);
	}
	
	public void load(String fileName){
		try{
			file = new RandomAccessFile(fileName, "rws");
			fc = file.getChannel();
		}
		catch(IOException e){
			e.printStackTrace();
			Debug.err("load myfile error");
		}
	}
	
	public void read(byte[] b, int block) throws IOException{
		file.seek(block * getPageSize());
		file.read(b);
	}
	
	public synchronized void read(ByteBuffer bytebuff, Integer pageID){
		try{
			bytebuff.clear();
			fc.read(bytebuff, pageID * getPageSize());
		} catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public synchronized void write(ByteBuffer bytebuff, Integer pageID){
		try{
			bytebuff.rewind();
			fc.write(bytebuff, pageID * getPageSize());
		} catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public synchronized int size(){
		try{
			return (int)(fc.size() / getPageSize());
		}
		catch(IOException e){
			e.printStackTrace();
			return 0;
		}
	}
	
	public void close(){
		try{
			if(file != null){
				file.close();
			}
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	public int getPageSize(){
		if(type == RECORD_FILE){
			return RECORD_PAGE_SIZE;
		}
		else if(type == BTREE_FILE){
			return BTREE_PAGE_SIZE;
		}
		else{
			Debug.err("Myfile without correct type");
			return -1;
		}
	}

	public void append(ByteBuffer buff) {
		int fromSize = size();
		write(buff, fromSize);
	}
}
