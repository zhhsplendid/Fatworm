package scan;

import util.Lib;
import value.BinaryOp;
import datatype.*;

public class Range {
	public long time;
	public DataRecord max;
	public DataRecord min;
	
	public Range(){
		min = null;
		max = null;
	}
	
	public Range(DataRecord one){
		min = one;
		max = one;
	}
	
	public Range(DataRecord small, DataRecord great){
		min = small;
		max = great;
	}
	
	public void intersect(Range r){
		if(Lib.isNull(min) || (!Lib.isNull(r.min) && min.cmp(BinaryOp.LESS, r.min))){
			min = r.min;
		}
		if(Lib.isNull(max) || (!Lib.isNull(r.max) && max.cmp(BinaryOp.GREATER, r.max))){
			max = r.max;
		}
	}
	
	//if max < min then is empty set
	public boolean isEmpty(){
		return !Lib.isNull(min) && !Lib.isNull(max) && min.cmp(BinaryOp.GREATER, max);
	}
	
	public int measureRange(){
		if(max instanceof Int && min instanceof Int){
			int maxvalue = ((Int)max).value;
			int minvalue = ((Int)min).value;
			int diff = maxvalue - minvalue;
			if(diff >= 0){
				return diff;
			}
		}
		if(!Lib.isNull(min) && !Lib.isNull(max) && min.cmp(BinaryOp.EQUAL, max)){
			return 0;
		}
		return Integer.MAX_VALUE;
	}
	
	@Override
	public String toString(){
		return "[" + min.toString() + ", " + max.toString() + "]";
	}
}
