package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import parser.FatwormParser;

public class Atom extends Expr{
	Tree tree;
	
	public Atom(Tree t){
		tree = t;
	}
	
	public void execute(){
		int type = tree.getType();
		switch(type){
		case FatwormParser.T__111:
			if(tree.getChildCount() == 1){
				TreeFormat.println("-");
				TreeFormat.up();
				Atom atom = new Atom(tree.getChild(0));
				atom.execute();
				TreeFormat.down();
			}
			else{
				Value value = new Value(tree);
				//TreeFormat.up();
				value.execute();
				//TreeFormat.down();
			}
			break;
			
		//subquery
		case FatwormParser.SELECT:
			Select select = new Select(tree);
			//TreeFormat.up();
			select.execute();
			//TreeFormat.down();
			break;
		case FatwormParser.SELECT_DISTINCT:
			SelectDistinct select_distinct = new SelectDistinct(tree);
			//TreeFormat.up();
			select_distinct.execute();
			//TreeFormat.down();
			break;
		
		//func
		case FatwormParser.AVG:
		case FatwormParser.COUNT:
		case FatwormParser.MIN:
		case FatwormParser.MAX:
		case FatwormParser.SUM:
			Func func = new Func(tree);
			func.execute();
			break;
		
		//const_value
		case FatwormParser.DEFAULT:
		case FatwormParser.NULL:
		case FatwormParser.TRUE:
		case FatwormParser.FALSE:
		case FatwormParser.STRING_LITERAL:
		case FatwormParser.INTEGER_LITERAL:
		case FatwormParser.FLOAT_LITERAL:
			ConstValue const_value = new ConstValue(tree);
			//TreeFormat.up();
			const_value.execute();
			//TreeFormat.down();
			break;
		
		//col_name
		case FatwormParser.ID:
		case FatwormParser.T__112:
			ColName col_name = new ColName(tree);
			//TreeFormat.up();
			col_name.execute();
			//TreeFormat.down();
			break;
		
		default:
			Value value = new Value(tree);
			//TreeFormat.up();
			value.execute();
			//TreeFormat.down();
		}
	}
	
}
