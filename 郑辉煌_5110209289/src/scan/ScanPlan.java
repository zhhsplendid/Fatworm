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
	
	final boolean opt = false;
	
	public static Scan getScan(Tree t, boolean optimize){
		if(t.getType() != FatwormParser.SELECT && t.getType()!=FatwormParser.SELECT_DISTINCT){
			return null;
		}
		boolean hasOrder = false;
		boolean hasGroup = false;
		boolean hasRename = false;
		boolean hasProjectAll = false;
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
					if(c.getType() == FatwormParser.DESC){
						ASCorDESC.add(c.getType());
						orderName.add(Lib.getAttributeName(c.getChild(0)));
					}
					else{
						ASCorDESC.add(FatwormParser.ASC);
						if(c.getType() == FatwormParser.ASC){
							orderName.add(Lib.getAttributeName(c.getChild(0)));
						}
						else{
							orderName.add(Lib.getAttributeName(c));
						}
					}
				}
				break;
			case FatwormParser.GROUP:
				hasGroup = true;
				groupName = Lib.getAttributeName(child.getChild(0));
				break;
			case FatwormParser.HAVING:
				having = Lib.getExpr(child.getChild(0));
				break;
			}
		}
		
		//
		//more to do
		List<Expr> expr = new ArrayList<Expr>();
		List<String> alias = new ArrayList<String>();
		
		for(int i = 0; i < t.getChildCount(); ++i){
			Tree child = t.getChild(i);
			if(child.getType() == FatwormParser.FROM){
				break;
			}
			if(child.getType() == FatwormParser.AS){
				hasAlias = true;
				Expr tmp = Lib.getExpr(child.getChild(0));
				
				if(tmp instanceof BinaryExpr){
					tmp = getLinearExpr((BinaryExpr) tmp);
				}
				hasRename = true;
				String rename = child.getChild(1).getText();
				expr.add(tmp);
				alias.add(rename);
				hasGroup |= tmp.hasAggr();
				
				for(int j = 0; j < orderName.size(); ++j){
					if(orderName.get(i).equalsIgnoreCase(rename)){
						orderName.set(i, tmp.toString());
					}
				}
				if(groupName != null && groupName.equalsIgnoreCase(rename)){
					//WARN
					groupName = tmp.toString();
				}
			}
			else if(child.getText().equals("*") && child.getChildCount() == 0){
				hasProjectAll = true;
			}
			else{
				Expr tmp = Lib.getExpr(child);
				if(tmp instanceof BinaryExpr){
					tmp = getLinearExpr((BinaryExpr) tmp);
				}
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
				whereCondition = new BinaryExpr(whereCondition, BinaryOp.AND, having);
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
		
		if(hasGroup){
			ans = new GroupScan(ans, expr, groupName, having, alias, hasAlias);
		}
		if(hasOrder){
			ans = new OrderScan(ans, expr, orderName, ASCorDESC);
		}
		if(! expr.isEmpty()){ //has project
			ans = new ProjectScan(ans, expr, hasProjectAll);
		}
		if(hasRename){
			ans = new RenameScan(ans, alias);
		}
		if(t.getType() == FatwormParser.SELECT_DISTINCT){
			ans = new DistinctScan(ans);
		}
		
		return whetherOptimize(ans, optimize);
	}
	
	private static Scan whetherOptimize(Scan scan, boolean optimize) {
		if(optimize){
			return Optimizer.optimize(scan);
		}
		return scan;
	}

	public static Scan getSourceScan(Tree t){
		Scan ret = null;
		for(int i = 0; i < t.getChildCount(); ++i){
			Tree child = t.getChild(i);
			Scan tmp = null;
			String newName = null;
			String oldName = null;
			if(child.getType() != FatwormParser.AS){
				oldName = child.getText();
			}
			else{
				if(child.getChild(0).getType() == FatwormParser.SELECT 
						|| child.getChild(0).getType() == FatwormParser.SELECT_DISTINCT){
					tmp = getScan(child.getChild(0), true);
				}
				else {
					oldName = child.getChild(0).getText();
				}
				newName = child.getChild(1).getText();
			} 
			
			if(tmp == null)
				tmp = new TableScan(oldName);
			if(newName != null)
				tmp = new RenameTableScan(tmp, newName);
			if(ret == null){
				ret = tmp;
			}
			else{
				ret = new JoinScan(ret, tmp);
			}
		}
		return ret;
	}
	
	public static Expr getLinearExpr(BinaryExpr input){
		Expr now = input;
		if(input.op != BinaryOp.PLUS){
			return input;
		}
		
		ArrayList<IdTimesExpr> l1 = new ArrayList<IdTimesExpr>();
		while(now instanceof BinaryExpr){
			if(((BinaryExpr)now).op == BinaryOp.PLUS){
				if(((BinaryExpr)now).left instanceof IdTimesExpr){
					l1.add((IdTimesExpr) ((BinaryExpr)now).left);
					now = ((BinaryExpr)now).right;
					continue;
				}
				if(((BinaryExpr)now).left instanceof IdExpr){
					l1.add(new IdTimesExpr((IdExpr) ((BinaryExpr)now).left, 1));
					now = ((BinaryExpr)now).right;
					continue;
				}
				if(((BinaryExpr)now).right instanceof IdTimesExpr){
					l1.add((IdTimesExpr) ((BinaryExpr)now).right);
					now = ((BinaryExpr)now).left;
					continue;
				}
				if(((BinaryExpr)now).right instanceof IdExpr){
					l1.add(new IdTimesExpr((IdExpr) ((BinaryExpr)now).right, 1));
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
		for(IdTimesExpr e:l1){
			ids.add(e.id);
			times.add(e.time);
		}
		return new LinearExpr(ids, times);
	}
}
