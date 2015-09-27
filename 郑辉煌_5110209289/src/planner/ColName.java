package planner;

import org.antlr.runtime.tree.Tree;

import parser.FatwormParser;

import output.TreeFormat;

public class ColName {
	Tree tree;
	String tbl_name;
	String attribute;
	boolean dot;
	
	public ColName(Tree t){
		tree = t;
		
		if(t.getType() == FatwormParser.T__112){
			tbl_name = t.getChild(0).getText();
			attribute = t.getChild(1).getText();
			dot = true;
		}
		else{
			tbl_name = null;
			dot = false;
			attribute = t.getText();
		}
	}
	
	public void execute(){
		if(dot){
			TreeFormat.println(".");
			TreeFormat.up();
			TreeFormat.println(tbl_name);
			TreeFormat.println(attribute);
			TreeFormat.down();
		}
		else{
			TreeFormat.println(attribute);
		}
	}
	
}
