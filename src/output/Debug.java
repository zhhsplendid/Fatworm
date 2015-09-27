package output;

import java.io.*;

import main.Main;

public class Debug {
	
	public static void print(String s){
		if(Main.debugMode){
			System.out.print(s);
		}
	} 
	
	public static void println(String s){
		if(Main.debugMode){
			System.out.println(s);
		}
	}
	
	public static void warn(String s){
		if(Main.debugMode){
			System.err.println(s);
		}
	}
	
	public static void err(String s){
		if(Main.debugMode){
			System.err.println(s);
		}
		System.exit(1);
	}
}
