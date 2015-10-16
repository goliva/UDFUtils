package general.udf;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;

@Description(name="array_contains",value="Check if exist an object exist into an array")
public class ArrayContains extends UDF {

	public Boolean evaluate(List list, Object o){
		return list.contains(o);
	}
	
	public BooleanWritable evaluate(ArrayWritable list, Writable o){
		return new BooleanWritable(evaluate(Arrays.asList(list),o.toString()));
	}
}
