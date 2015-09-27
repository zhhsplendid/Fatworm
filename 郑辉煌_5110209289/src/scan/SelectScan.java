package scan;

import java.util.LinkedList;
import java.util.List;

import util.Env;
import util.Lib;
import value.*;
import fatworm.driver.Record;
import fatworm.driver.Schema;


public class SelectScan extends Scan {
	
	public Scan fromScan;
	
	public Expr expr;
	public Env env;
	
	public boolean hasPush;
	public boolean skip;
	
	public Record now;
	
	List<String> nameList, offsetList;
	
	public SelectScan(Scan from, Expr e){
		super();
		fromScan = from;
		fromScan.toScan = this;
		
		if(e instanceof BinaryExpr){
			expr = ((BinaryExpr) e).toCNF();
		}
		else{
			expr = e;
		}
		
		
		aggregation.addAll(fromScan.getAggregation());
		skip = false;
		hasPush = false;
	}
	
	public boolean isPushable(){
		if(expr.hasSubquery()){
			return false;
		}
		
		if(fromScan instanceof ProjectScan)
			return ((ProjectScan)fromScan).isConst();
		if(fromScan instanceof SelectScan || fromScan instanceof RenameTableScan )//||src instanceof Rename)
			return true;
		if(fromScan instanceof JoinScan)
			return Lib.isSubset(expr.requestCol(), ((JoinScan)fromScan).left.getCol()) 
					|| Lib.isSubset(expr.requestCol(), ((JoinScan)fromScan).right.getCol());
		
		//Debug.warn("Select isPushable meow!!!");
		return false;
	}
	
	@Override
	public void beforeFirst() {
		fromScan.beforeFirst();
		now = null;
		getNext();
	}
	
	private void getNext(){
		if(skip){
			now = fromScan.hasNext() ? fromScan.next(): null;
			return;
		}
		else{
			while(fromScan.hasNext()){
				Record record = fromScan.next();
				Env tmp = env.clone();
				tmp.appendFromRecord(record);//
				if(expr.valuePredicate(tmp)){
					now = record;
					return;
				}
			}
			now = null;
		}
	}
	
	@Override
	public Record next() {
		Record ans = now;
		getNext();
		return ans;
	}

	@Override
	public boolean hasNext() {
		return now != null;
	}

	@Override
	public String toString() {
		return "select ( " + fromScan.toString() + ")";
	}

	@Override
	public void close() {
		fromScan.close();
	}

	@Override
	public Schema getSchema() {
		return fromScan.getSchema();
	}

	@Override
	public void eval(Env env) {
		hasComputed = true;
		now = null;
		fromScan.eval(env);
		this.env = env;
		getNext();
	}

	@Override
	public void rename(String oldName, String newName) {
		fromScan.rename(oldName, newName);
		if(expr.getType(fromScan.getSchema()) == java.sql.Types.NULL){
			expr.rename(oldName, newName);
		}
	}

	@Override
	public List<String> getCol() {
		return new LinkedList<String>(fromScan.getCol());
	}

	@Override
	public List<String> requestCol() {
		List<String> list = fromScan.requestCol();
		list.removeAll(fromScan.getCol());
		Lib.addAllCol(list, expr.requestCol());
		return list;
	}

}
