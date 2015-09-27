package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class IntegerLiteral extends ConstValue{
	Integer value;
	
	public IntegerLiteral(Tree t){
		super(t);
		value = new Integer(t.getText());
	}
	
	public void execute(){
		
		TreeFormat.println("INTEGER_LITERAL");
		TreeFormat.up();
		TreeFormat.println(value.toString());
		TreeFormat.down();
	}
}
