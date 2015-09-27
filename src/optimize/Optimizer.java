package optimize;

import java.util.*;

import scan.*;
import util.Lib;
import value.*;

public class Optimizer {
	
	public static final String tablePrefix = "__test___";
	
	public static Set<String> sortOnField;// = new HashSet<String>();
	
	public static Scan optimize(Scan scan){
		cleanSelect(scan);
		scan = pushSelect(decomposeAnd(scan));
		transformTheta(scan);
		sortOnField = new HashSet<String>();
		return clearInnerOrders(scan);
	}

	private static void cleanSelect(Scan scan) {
		if(scan instanceof ProjectScan){
			ProjectScan proj = (ProjectScan)scan;
			List<Expr> expr = proj.exprs;
			int size = expr.size();
			for(int i=0; i < size; ++i){
				Expr e = expr.get(i);
				if(e instanceof QueryExpr && ((QueryExpr)e).scan instanceof ProjectScan){
					QueryExpr qe = (QueryExpr)e;
					ProjectScan tmp = (ProjectScan) (qe).scan;
					if(tmp.fromScan instanceof VirtualScan){
						Expr tmpe = tmp.exprs.get(0);
						expr.set(i, tmpe);
					}
				}
			}
			cleanSelect(proj.fromScan);
		}
		else if(scan instanceof SelectScan){
			SelectScan select = (SelectScan)scan;
			cleanSelect(select.fromScan);
			boolean isInExpr = select.expr instanceof InExpr;
			if(isInExpr){
				InExpr tmp1 = (InExpr) select.expr;
				boolean isProject = (tmp1.scan instanceof ProjectScan);
				if(isProject){
					ProjectScan tmp2 = (ProjectScan) tmp1.scan;
					String s1 = tmp1.expr.toString();
					String s2 = tmp2.exprs.get(0).toString();
					if(tmp2.fromScan instanceof TableScan && 
							tmp2.exprs.get(0) instanceof IdExpr 
							&& 
							(!Lib.getAttributeName(s1).equalsIgnoreCase(Lib.getAttributeName(s2))
									||tmp1.expr.toString().contains("."))){
						
						String tbl = tablePrefix + tmp2.getSchema().tableName;
						select.setFrom(new JoinScan(select.fromScan, new RenameTableScan(tmp2, tbl)));
						String tmps = tmp2.exprs.get(0).toString();
						String col0 = tbl + "." + Lib.getAttributeName(tmps);
						IdExpr tmpid = new IdExpr(col0);
						select.expr = new BinaryExpr(tmp1.expr, tmpid, BinaryOp.EQUAL);
					}
				}
			}
		}
		else if(scan instanceof OneFromScan){
			cleanSelect(((OneFromScan) scan).fromScan);
		}
		else if(scan instanceof TwoFromScan){
			TwoFromScan two = (TwoFromScan) scan;
			cleanSelect(two.left);
			cleanSelect(two.right);
		}
	}

	private static Scan clearInnerOrders(Scan scan) {
		
		if(scan instanceof SortScan){
			SortScan sort = (SortScan)scan;
			boolean duplicate = !sortOnField.isEmpty();
			for(String x: sort.orderName){
				//FIXME should check here and do rename on Rename & RenameTable instead 
				String s = Lib.getAttributeName(x).toLowerCase();
				if(!sortOnField.contains(s)){
					sortOnField.add(s);
				}
			}
			if(!duplicate){
				sort.setFrom(clearInnerOrders(sort.fromScan));
				return sort;
			}
			else{
				sort.fromScan = clearInnerOrders(sort.fromScan);
				sort.fromScan.toScan = sort.toScan;
				return sort.fromScan;
			}
		}
		else if(scan instanceof OneFromScan){
			OneFromScan one = (OneFromScan) scan;
			one.setFrom(clearInnerOrders(one.fromScan));
			return one;
		}
		else if(scan instanceof TwoFromScan){
			TwoFromScan two = (TwoFromScan) scan;
			two.left = clearInnerOrders(two.left);
			two.right = clearInnerOrders(two.right);
			two.left.toScan = two;
			two.right.toScan = two;
			return two;
		}
		else if(scan instanceof ZeroFromScan){
			return scan;
		}
		return null;
	}

	private static Scan pushSelect(Scan scan) {
		if(scan instanceof SelectScan){
			SelectScan select = (SelectScan)scan;
			if(select.hasPush){
				select.setFrom(pushSelect(select.fromScan));
				return select;
			}
			Scan from = select.fromScan;
			Scan head = select;
			SelectScan cur = select;
			
			while(cur.isPushable()){
				
				if(cur.fromScan instanceof RenameScan){
					RenameScan curChild = (RenameScan) cur.fromScan;
					int size = curChild.asNewName.size();
					for(int i = 0; i < size; ++i){
						//String oldName = curChild.asNewName.get(i);
						//String newName = curChild.fromScan.getCol().get(i);
						cur.expr.rename(curChild.asNewName.get(i), curChild.fromScan.getCol().get(i));
					}
				}
				else if(cur.fromScan instanceof RenameTableScan){
					RenameTableScan curChild = (RenameTableScan) cur.fromScan;
					String tbl = curChild.alia;
					List<String> col = curChild.fromScan.getCol();
					for(String newName : col){
						//String oldName = tbl + "." + Lib.getAttributeName(newName);
						cur.expr.rename(tbl + "." + Lib.getAttributeName(newName), newName);
					}
				}
				push(cur);
				if(head == select){
					head = from;
				}
			}
			cur.hasPush = true;
			return pushSelect(head);
		}
		else if(scan instanceof OneFromScan){
			OneFromScan one = (OneFromScan)scan;
			one.setFrom(pushSelect(one.fromScan));
			return one;
		}
		else if(scan instanceof TwoFromScan){
			TwoFromScan two = (TwoFromScan)scan;
			two.left = pushSelect(two.left);
			two.right = pushSelect(two.right);
			two.left.toScan = two;
			two.right.toScan = two;
			return two;
		}
		return scan;
	}

	private static void push(SelectScan cur) {
		Scan to = cur.toScan;
		Scan from = cur.fromScan;
		if(to == null)	
			from.toScan = null;
		else{
			to.setFrom(cur, from);
		}
		
		if(from instanceof JoinScan){
			JoinScan join = (JoinScan)from;
			List<String> reqCol = cur.expr.requestCol(); //maybe wrong
			Scan oldFrom, newFrom;
			if(Lib.isSubset(reqCol, join.left.getCol())){
				oldFrom = cur.fromScan;
				newFrom = join.left;
				cur.setFrom(oldFrom, newFrom);
				from.setFrom(newFrom, cur);
			}
			else if(Lib.isSubset(reqCol, join.right.getCol())){
				oldFrom = cur.fromScan;
				newFrom = join.right;
				cur.setFrom(oldFrom, newFrom);
				from.setFrom(newFrom, cur);
			}
		}
		else if(from instanceof GroupScan){
			
		}//FIXME
		else if(from instanceof OneFromScan){
			OneFromScan one = (OneFromScan) from;
			cur.setFrom(from, one.fromScan);
			from.setFrom(one.fromScan, cur);
		}
		
	}

	private static void transformTheta(Scan scan) {
		if(scan instanceof JoinScan){
			JoinScan join = (JoinScan)scan;
			Scan l = join.left;
			Scan r = join.right;
			transformTheta(l);
			transformTheta(r);
			
			Scan to = join.toScan;
			Scan cur = join;
			boolean tmpb = !(to instanceof SelectScan);
			if(tmpb){
				return;
			}
			
			ThetaJoinScan ans = new ThetaJoinScan(join);
			while(to instanceof SelectScan){
				Expr e = ((SelectScan)to).expr;
				ans.add(e);
				cur = to;
				to = to.toScan;
			}
			boolean notNull = (to != null);
			if(notNull){
				to.setFrom(cur, ans);
			}
			ans.toScan = to;
		}
		else if(scan instanceof ThetaJoinScan){
			ThetaJoinScan p = (ThetaJoinScan)scan;
			transformTheta(p.left);
			transformTheta(p.right);
		}
		else if(scan instanceof OneFromScan){
			transformTheta(((OneFromScan) scan).fromScan);
		}
	}

	private static Scan decomposeAnd(Scan scan) {
		
		if(scan instanceof SelectScan){
			SelectScan select = (SelectScan)scan;
			select.setFrom(decomposeAnd(select.fromScan));
			if(!Lib.isAnd(select.expr)){
				return select;
			}
			LinkedList<Expr> exprList = new LinkedList<Expr>();
			Lib.collectCondition(exprList, select.expr);
			
			HashSet<Expr> toAdd;
			boolean notClosed = true;
			while(notClosed){
				notClosed = false;
				toAdd = new HashSet<Expr>();
				int size = exprList.size();
				for(int i = 0; i < size; ++i){
					Expr xxx = exprList.get(i);
					if(!((xxx instanceof BinaryExpr) && ((BinaryExpr)xxx).op == BinaryOp.EQUAL)){
						continue;
					}
					BinaryExpr xx = (BinaryExpr) xxx;
					IdExpr x1 = null, x2 = null;
					if(xx.left instanceof IdExpr){
						x1 = (IdExpr)xx.left;
					}
					if(xx.right instanceof IdExpr){
						x2 = (IdExpr)xx.right;
					}
					
					if(x1 == null && x2 == null){
						continue;
					}
					for(int j = i + 1; j < size; ++j){
						Expr yyy = exprList.get(j);
						if(!((yyy instanceof BinaryExpr) && ((BinaryExpr)yyy).op==BinaryOp.EQUAL)){
							continue;
						}
						BinaryExpr yy = (BinaryExpr) yyy;
						IdExpr y1 = null, y2 = null;
			
						if(yy.left instanceof IdExpr){
							y1 = (IdExpr)yy.left;
						}
						if(yy.right instanceof IdExpr){
							y2 = (IdExpr)yy.right;
						}
						
						boolean allNull = y1==null && y2==null;
						if(allNull){
							continue;
						}
						if(x1 != null && y1!=null && x1.name.equalsIgnoreCase(y1.name)){
							toAdd.add(new BinaryExpr(x1, yy.right, BinaryOp.EQUAL));
							toAdd.add(new BinaryExpr(y1, xx.right, BinaryOp.EQUAL));
							toAdd.add(new BinaryExpr(yy.right, xx.right, BinaryOp.EQUAL));
						}
						else if(x2 != null && y1 != null && x2.name.equalsIgnoreCase(y1.name)){
							toAdd.add(new BinaryExpr(x2, yy.right, BinaryOp.EQUAL));
							toAdd.add(new BinaryExpr(y1, xx.left, BinaryOp.EQUAL));
							toAdd.add(new BinaryExpr(yy.right, xx.left, BinaryOp.EQUAL));
						}
						else if(x1 != null && y2 != null && x1.name.equalsIgnoreCase(y2.name)){
							toAdd.add(new BinaryExpr(x1, yy.left, BinaryOp.EQUAL));
							toAdd.add(new BinaryExpr(y2, xx.right, BinaryOp.EQUAL));
							toAdd.add(new BinaryExpr(yy.left, xx.right, BinaryOp.EQUAL));
						}
						else if(x2 != null && y2 != null && x2.name.equalsIgnoreCase(y2.name)){
							toAdd.add(new BinaryExpr(x2, yy.left, BinaryOp.EQUAL));
							toAdd.add(new BinaryExpr(y2, xx.left, BinaryOp.EQUAL));
							toAdd.add(new BinaryExpr(yy.right, xx.left, BinaryOp.EQUAL));
						}
					}
				}
				toAdd.addAll(exprList);
				notClosed = size < toAdd.size();
				/*
				if(notClosed){
					Debug.warn("Generating new expressions!!! in Optimize.java");
				}*/
				exprList = new LinkedList<Expr>(toAdd);
			}
			Scan ans = select.fromScan;
			Iterator<Expr> iter = exprList.iterator();
			while(iter.hasNext()){
				Expr e = iter.next();
				ans = new SelectScan(ans, e);
			}
			ans.toScan = select.toScan;
			return ans;
		}
		else if(scan instanceof OneFromScan){
			OneFromScan one = (OneFromScan) scan;
			one.setFrom(decomposeAnd(one.fromScan));
			return one;
		}
		else if(scan instanceof TwoFromScan){
			TwoFromScan two = (TwoFromScan) scan;
			two.left = decomposeAnd(two.left);
			two.right = decomposeAnd(two.right);
			two.left.toScan = two;
			two.right.toScan = two;
			return two;
		}
		else if(scan instanceof ZeroFromScan){
			return scan;
		}
		return null;
	}
}
