package planner;

import org.antlr.runtime.tree.Tree;

public class Token {
	Tree tree;
	
	public Token(Tree t){
		tree = t;
	}
	
	public String getText(){
		return tree.getText();
	}
	
	public void show(){
		System.out.print(tree.getText());
	}
	
	public void showln(){
		System.out.println(tree.getText());
	}
}
