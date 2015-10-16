package general.udaf.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ArrayConcatEntry implements Writable{

	Comparable comp;
	Object obj;
	
	
	
	public ArrayConcatEntry(Comparable comp, Object obj) {
		super();
		this.comp = comp;
		this.obj = obj;
	}
	
	public Comparable getComp() {
		return comp;
	}
	public void setComp(Comparable comp) {
		this.comp = comp;
	}
	public Object getObj() {
		return obj;
	}
	public void setObj(Object obj) {
		this.obj = obj;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	
}
