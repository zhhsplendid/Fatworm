package scan;

import java.util.*;

import datatype.DataRecord;
import util.Env;
import value.BinaryExpr;
import value.BinaryOp;
import value.Expr;
import value.IdExpr;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public class ThetaJoinScan extends TwoFromScan {
	
	//public Scan left;
	//public Scan right;
	public LinkedList<Expr> condList = new LinkedList<Expr>();
	public Record curLeft;
	public Record current;
	public Schema schema;
	private Env env;
	
	private ArrayList<Record> result = new ArrayList<Record>();
	
	private String leftName;
	private String rightName;
	private boolean hasMergeJoin = false;
	private int pointer = 0;
	
	public ThetaJoinScan(JoinScan js) {
		left = js.left;
		left.toScan = this;
		right = js.right;
		right.toScan = this;
		curLeft = null;
		aggregation = js.getAggregation();
		schema = js.getSchema();
	}
	
	private BinaryExpr findMergeJoinExpr(){
		for(Expr e: condList){
			if(e instanceof BinaryExpr){
				BinaryExpr b = (BinaryExpr) e;
				boolean isMergeJoin = (b.op == BinaryOp.EQUAL && b.left instanceof IdExpr && b.right instanceof IdExpr);
				if(isMergeJoin){
					return b;
				}
			}
		}
		return null;
	}
	
	public void add(Expr expr) {
		condList.add(expr);
	}
	
	@Override
	public void beforeFirst() {
		curLeft = null;
		left.beforeFirst();
		right.beforeFirst();
		pointer = 0;
		if(!hasMergeJoin){
			current = getNext();
		}
	}
	
	private Record getNext() {
		Record ans = null;
		
		while((curLeft != null || left.hasNext()) && right.hasNext()){
			Record r = right.next();
			if(curLeft == null){
				curLeft = left.next();
			}
			Record tmp = joinTwoRecords(curLeft, r);
			if(! right.hasNext() && left.hasNext()){
				right.beforeFirst();
				curLeft = left.next();
			}
			if(test(tmp)){
				ans = tmp;
				break;
			}
		}
		return ans;
	}
	
	private boolean test(Record ans) {
		Env localEnv = env.clone();
		localEnv.appendFromRecord(ans);
		for(Expr e: condList){
			boolean satisfy = e.valuePredicate(localEnv);
			if(!satisfy){
				return false;
			}
		}
		return true;
	}

	private Record joinTwoRecords(Record l, Record r){
		Record ans = new Record(schema);
		ans.addField(l.cols);
		ans.addField(r.cols);
		return ans;
	}
	
	@Override
	public Record next() {
		if(hasMergeJoin){
			return result.get(pointer++);
		}
		else{
			Record ans = current;
			current = getNext();
			return ans;
		}
	}

	

	@Override
	public boolean hasNext() {
		if(hasMergeJoin){
			return pointer < result.size();
		}
		else{
			return current != null;
		}
	}

	@Override
	public String toString() {
		return "ThetaJoin(" + left.toString() +"," + right.toString() + ")";
	}

	@Override
	public void close() {
		left.close();
		right.close();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void eval(Env env) {
		hasComputed = true;
		BinaryExpr merge = findMergeJoinExpr();
		curLeft = null;
		this.env = env;
		
		Schema lsche = left.getSchema();
		Schema rsche = right.getSchema();
		if(merge != null){
			
			String lmerge = merge.left.toString();
			String rmerge = merge.right.toString();
			if(lsche.indexOfStrictString(lmerge) >= 0 && rsche.indexOfStrictString(rmerge) >= 0){
				leftName = lmerge;
				rightName = rmerge;
				hasMergeJoin = true;
			}
			else if(rsche.indexOfStrictString(lmerge) >= 0 && lsche.indexOfStrictString(rmerge)>=0) {
				leftName = rmerge;
				rightName = lmerge;
				hasMergeJoin = true;
			}
			else{
				hasMergeJoin = false;
			}
		}
		
		left.eval(env);//left.env(this.env);
		right.eval(env); //right.env(this.env);
		
		if(hasMergeJoin){
			LinkedList<Record> llist = new LinkedList<Record>();
			LinkedList<Record> rlist = new LinkedList<Record>();
			while(left.hasNext()){
				llist.add(left.next());
			}
			if(llist.isEmpty()){
				return;
			}
			while(right.hasNext()){
				rlist.add(right.next());
			}
			if(rlist.isEmpty()){
				return;
			}
			
			int lindex = lsche.indexOf(leftName);
			sort(llist, lindex);
			
			int rindex = rsche.indexOf(rightName);
			sort(rlist, rindex);
			
			Record curl = llist.pollFirst();
			Record curr = rlist.pollFirst();
			DataRecord lval = curl.getField(lindex);
			DataRecord rval = curr.getField(rindex);
			
			ArrayList<Record> lastRight = new ArrayList<Record>();
			DataRecord lastr = null;
			
			while(true){
				lval = curl.getField(lindex);
				if(lastr != null && lval.cmp(BinaryOp.EQUAL, lastr)){
					int size = lastRight.size();
					for(int i = 0; i < size; ++i){
						Record r = lastRight.get(i);
						Record tmp = joinTwoRecords(curl, r);
						if(test(tmp)){
							result.add(tmp);
						}
					}
				}
				else{
					while(lval.cmp(BinaryOp.GREATER, rval) && !rlist.isEmpty()){
						curr = rlist.pollFirst();
						rval = curr.getField(rindex);
					}
					
					lastRight = new ArrayList<Record>();
					
					while(lval.cmp(BinaryOp.EQUAL, rval)){
						Record tmp = joinTwoRecords(curl, curr);
						boolean can = test(tmp);
						if(can){
							result.add(tmp);
						}
						lastRight.add(curr);
						boolean empty = rlist.isEmpty();
						if(empty){
							break;
						}
						curr = rlist.pollFirst();
						rval = curr.getField(rindex);
					}
					lastr = lval;
				}
				
				boolean empty = llist.isEmpty();
				if(empty){
					break;
				}
				curl = llist.pollFirst();
			}
		}
		else{
			current = getNext();
		}
		
		
	}
	
	private void sort(List<Record> toSort, final int index){
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");   
		Collections.sort(toSort, new Comparator<Record>(){
			public int compare(Record a, Record b){
				DataRecord l = a.getField(index);
				DataRecord r = b.getField(index);
				if(l.cmp(BinaryOp.LESS, r)){
					return -1;
				}
				else if(l.cmp(BinaryOp.GREATER, r)){
					return 1;
				}
				return 0;
			}
		});
	}
	
	
	
	@Override
	public void rename(String oldName, String newName) {
		left.rename(oldName, newName);
		right.rename(oldName, newName);
	}

	@Override
	public List<String> getCol() {
		List<String> l = left.getCol();
		List<String> r = right.getCol();
		List<String> list = new LinkedList<String> (l);
		list.addAll(r);
		return list;
	}

	@Override
	public List<String> requestCol() {
		List<String> l = left.requestCol();
		List<String> r = right.requestCol();
		List<String> list = new LinkedList<String> (l);
		list.addAll(r);
		return list;
	}

	

}
