package output;

import java.io.*;

public class Output {
	static String fileName = "hello.txt";
	static FileWriter fw;
	
	public Output(String path){
		fileName = path;
		try{
			fw = new FileWriter(path);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public static void init() throws IOException{
		if(fw == null){
			fw = new FileWriter(fileName);
		}
	}
	
	public static void filePrintln(String s){
		try{
			init();
			fw.write(s,0,s.length());
			fw.write("\n");
			fw.flush();  
		}catch(IOException e){
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public static void filePrint(String s){
		try{
			init();
			fw.write(s,0,s.length());
			fw.flush();  
		}catch(IOException e){
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public static void print(String s){
		filePrint(s);
		System.out.print(s);
	}  
	
	public static void println(String s){
		filePrintln(s);
		System.out.println(s);
	}
	
	
}
