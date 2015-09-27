package output;

public class TreeFormat {
	static private int tab = 0;
	
	static public void up(){
		++tab;
	}
	static public void down(){
		--tab;
		assert(tab > 0);
	}
	
	static public void up(int x){
		tab = tab + x;
	}
	
	static public void down(int x){
		tab = tab - x;
		assert(tab > 0);
	}
	
	static public void println(String out){
		String blank = "";
		for(int i = 1; i < tab; ++i){
			blank = blank + "\t|";
		}
		if(tab >= 1){
			blank = blank + "\t|--::";
		}
		Output.print(blank);
		Output.println(out);
	}
}
