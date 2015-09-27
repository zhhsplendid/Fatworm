package scan;


import java.util.ArrayList;
import java.util.List;

import optimize.Optimizer;

import org.antlr.runtime.tree.Tree;





import parser.FatwormParser;
import util.Env;
import util.Lib;
import value.BinaryExpr;
import value.BinaryOp;
import value.Expr;
import value.IdExpr;
import value.IdTimesExpr;
import value.LinearExpr;

public class ScanPlan {
	public long time;
	final static boolean opt = true;
	
	//diffcult: data select a as b from c order by b
		//Scan order:
		// with aggregation: Distinct <- Rename <- Project <- Order <- Group <- Select <- source
		// without :	Distinct <- Rename <- Project <- Order <- Select <- source
	
	public static Scan getScan(Tree t, boolean optimize){
		if(t.getType() != FatwormParser.SELECT && t.getType() != FatwormParser.SELECT_DISTINCT){
			return null;
		}
		boolean hasDistinct = t.getType() == FatwormParser.SELECT_DISTINCT;
		boolean hasOrder = false;
		boolean hasGroup = false;
		boolean hasRename = false;
		boolean hasProjectAll = false;
		boolean hasProject;
		boolean hasAlias = false;
		
		String groupName = null;
		Scan ans = null, sourceScan = null;
		Expr whereCondition = null, having = null;
		
		//two for order information
		List<String> orderName = new ArrayList<String>();
		List<Integer> ASCorDESC = new ArrayList<Integer>();
		
		
		for(int i = 0; i < t.getChildCount(); ++i){
			Tree child = t.getChild(i);
			switch(child.getType()){
			case FatwormParser.FROM:
				sourceScan = getSourceScan(child);
				break;
			case FatwormParser.WHERE:
				whereCondition = Lib.getExpr(child.getChild(0));
				break;
			case FatwormParser.ORDER:
				hasOrder = true;
				for(int j = 0; j < child.getChildCount(); ++j){
					Tree c = child.getChild(j);
					if(c.getType() != FatwormParser.DESC){
						ASCorDESC.add(FatwormParser.ASC);
						if(c.getType() == FatwormParser.ASC){
							orderName.add(Lib.getAttributeName(c.getChild(0)));
						}
						else{
							orderName.add(Lib.getAttributeName(c));
						}
					}
					else{
						ASCorDESC.add(FatwormParser.DESC);
						orderName.add(Lib.getAttributeName(c.getChild(0)));
					}
				}
				break;
			case FatwormParser.HAVING:
				having = Lib.getExpr(child.getChild(0));
				break;
			case FatwormParser.GROUP:
				hasGroup = true;
				groupName = Lib.getAttributeName(child.getChild(0));
				break;
			
			}
		}
		
		//
		//more to do
		List<Expr> expr = new ArrayList<Expr>();
		List<String> alias = new ArrayList<String>();
		int count = t.getChildCount();
		for(int i = 0; i < count; ++i){
			Tree child = t.getChild(i);
			if(child.getType() == FatwormParser.FROM){
				break;
			}
			if(child.getType() == FatwormParser.AS){
				hasAlias = true;
				Expr tmp = Lib.getExpr(child.getChild(0));
				/*
				if(tmp instanceof BinaryExpr){
					tmp = getLinearExpr((BinaryExpr) tmp);
				}*/
				hasRename = true;
				String rename = child.getChild(1).getText();
				expr.add(tmp);
				alias.add(rename);
				hasGroup |= tmp.hasAggr();
				
				for(int j = 0; j < orderName.size(); ++j){
					if(orderName.get(j).equalsIgnoreCase(rename)){
						orderName.set(j, tmp.toString());
					}
				}
				if(groupName != null && groupName.equalsIgnoreCase(rename)){
					//WARN
					groupName = tmp.toString();
				}
			}
			else if(child.getChildCount() == 0 && child.getText().equals("*")){
				hasProjectAll = true;
			}
			else{
				Expr tmp = Lib.getExpr(child);
				/*
				if(tmp instanceof BinaryExpr){
					tmp = getLinearExpr((BinaryExpr) tmp);
				}*/
				alias.add(tmp.toString());
				expr.add(tmp);
				
				hasGroup |= tmp.hasAggr();
			}
			
			
			
		}
		//
		
		if(having != null){
			hasGroup |= having.hasAggr();
		}
		
		if(sourceScan == null){
			sourceScan = new VirtualScan(); // deal with select 1 + 1;
		}
		
		ans = sourceScan;
		if(! hasGroup){
			if(whereCondition == null){
				whereCondition = having;
			}
			else if(having != null){
				whereCondition = new BinaryExpr(whereCondition, having, BinaryOp.AND);
			}
		}
		
		if(whereCondition != null){
			if(! whereCondition.isConst){
				ans = new SelectScan(sourceScan, whereCondition);
			}
			else if(! whereCondition.valuePredicate(new Env())){
				ans = new SelectScan(sourceScan, whereCondition);
			}
		}
		
		hasProject = !expr.isEmpty();
		
		if(hasGroup){
			ans = new GroupScan(ans, groupName, having, hasAlias, expr, alias);
		}
		if(hasOrder){
			ans = new SortScan(ans, orderName, expr,  ASCorDESC);
		}
		if(hasProject){ //has project
			ans = new ProjectScan(ans, expr, hasProjectAll);
		}
		if(hasRename){
			ans = new RenameScan(ans, alias);
		}
		if(hasDistinct){
			ans = new DistinctScan(ans);
		}
		
		return whetherOptimize(ans, optimize);
	}
	

	private static Scan whetherOptimize(Scan scan, boolean optimize) {
		if(optimize && opt){
			return Optimizer.optimize(scan);
		}
		return scan;
	}

	public static Scan getSourceScan(Tree t){
		Scan ans = null;
		int count = t.getChildCount();
		boolean fromTable;
		boolean hasNewName;
		for(int i = 0; i < count; ++i){
			Tree child = t.getChild(i);
			Scan tmp = null;
			String newName = null;
			String oldName = null;
			if(child.getType() != FatwormParser.AS){
				oldName = child.getText();
			}
			else{
				if(child.getChild(0).getType() != FatwormParser.SELECT 
						&& child.getChild(0).getType() != FatwormParser.SELECT_DISTINCT){
					oldName = child.getChild(0).getText();
				}
				else {
					tmp = getScan(child.getChild(0), true);
				}
				newName = child.getChild(1).getText();
				
			} 
			fromTable = tmp == null;
			hasNewName = newName != null;
			if(fromTable){
				tmp = new TableScan(oldName);
			}
			if(hasNewName){
				tmp = new RenameTableScan(tmp, newName);
			}
			
			if(ans != null){
				ans = new JoinScan(ans, tmp);
			}
			else{
				ans = tmp;
			}
		}
		return ans;
	}
	
	/*
	public static Expr getLinearExpr(BinaryExpr input){
		Expr now = input;
		if(input.op != BinaryOp.PLUS){
			return input;
		}
		
		ArrayList<IdTimesExpr> l1 = new ArrayList<IdTimesExpr>();
		while(now instanceof BinaryExpr){
			IdTimesExpr tmp;
			if(((BinaryExpr)now).op == BinaryOp.PLUS){
				if(((BinaryExpr)now).left instanceof IdTimesExpr){
					tmp = (IdTimesExpr) ((BinaryExpr)now).left;
					l1.add(tmp);
					now = ((BinaryExpr)now).right;
					continue;
				}
				if(((BinaryExpr)now).left instanceof IdExpr){
					tmp = new IdTimesExpr((IdExpr) ((BinaryExpr)now).left, 1);
					l1.add(tmp);
					now = ((BinaryExpr)now).right;
					continue;
				}
				if(((BinaryExpr)now).right instanceof IdTimesExpr){
					tmp = (IdTimesExpr) ((BinaryExpr)now).right;
					l1.add(tmp);
					now = ((BinaryExpr)now).left;
					continue;
				}
				if(((BinaryExpr)now).right instanceof IdExpr){
					tmp = new IdTimesExpr((IdExpr) ((BinaryExpr)now).right, 1);
					l1.add(tmp);
					now = ((BinaryExpr)now).left;
					continue;
				}
			}
			break;
		}
		if(now instanceof IdTimesExpr){
			l1.add((IdTimesExpr) now);
		}else{
			return input;
		}
		if(l1.isEmpty()){
			return input;
		}
		ArrayList<Expr> ids = new ArrayList<Expr> ();
		ArrayList<Integer> times = new ArrayList<Integer>();
		int lsize = l1.size();
		for(int i = 0; i < lsize; ++i){
			IdTimesExpr e = l1.get(i);
			ids.add(e.id);
			times.add(e.time);
		}
		return new LinearExpr(ids, times);
	}*/
}
