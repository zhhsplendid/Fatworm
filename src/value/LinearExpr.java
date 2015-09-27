package value;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import output.Debug;
import util.Env;
import util.Lib;
import datatype.*;
import fatworm.driver.Record;

public class LinearExpr extends Expr {
	public static long time;
	String name;
	public ArrayList<Expr> ids;
	public ArrayList<Integer> times;
	LinkedList<String> request;
	public LinearExpr(ArrayList<Expr> id, ArrayList<Integer> time) {
		ids = id;
		times = time;
		size = ids.size() + 1;
	}
	
	private int multi( DataRecord rd, Integer i){
		if(rd.type != java.sql.Types.INTEGER){
			Debug.err("multi err in LinearExpr.java");
			return 0;
		}
		return ((Int)rd).value * i;
		
	}
	
	public DataRecord valueByIndex(Record record){
		if(isConst){
			return value;
		}
		int ans = 0;
		int size = times.size();
		for(int i = 0; i < size; ++i){
			ans += multi( ((IdExpr)ids.get(i)).valueByIndex(record), times.get(i));
		}
		return new Int(ans);
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		return Lib.toBoolean(valueExpr(env));
	}

	@Override
	public DataRecord valueExpr(Env env) {
		if(isConst){
			return value;
		}
		else{
			int ans = 0;
			int size = times.size();
			for(int i = 0; i < size; ++i){
				DataRecord d = ids.get(i).valueExpr(env);
				Integer c = times.get(i);
				ans += multi(d, c);
			}
			return new Int(ans);
		}
	}

	@Override
	public List<String> requestCol() {
		if(request == null){
			request = new LinkedList<String>();
			int isize = ids.size();
			for(int i = 0; i < isize; ++i){
				Expr expr = ids.get(i);
				request.addAll(expr.requestCol());
			}
		}
		return request;
	}

	@Override
	public void rename(String oldName, String newName) {
		for(int i = 0; i < ids.size(); ++i){
			Expr id = ids.get(i);
			id.rename(oldName, newName);
		}
	}

	@Override
	public boolean hasSubquery() {
		return false;
	}

	@Override
	public Expr clone() {
		return new LinearExpr(ids, times);
	}

}
