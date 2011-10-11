package general.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@Description(name="array_to_string", value="_FUNC_(array,separator) - Generate an string from an array")
public class ArrayToString extends GenericUDF {
	
	Text retText = new Text();
	private ListObjectInspector listInspector;
	private PrimitiveObjectInspector stringInspector;

	@Override
	public Object evaluate(DeferredObject[] record) throws HiveException {
		List l = listInspector.getList(record[0].get());
		String separator = (String)stringInspector.getPrimitiveJavaObject(record[1].get());
		StringBuffer sb = new StringBuffer();
		for(Object o : l){
			sb.append(separator);
			sb.append(o.toString());
		}
		retText.set(new Text(sb.toString().substring(separator.length())));
		return retText;
	}


	@Override
	public String getDisplayString(String[] arg0) {
		return "Array To String";
	}


	@Override
	public ObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		if(args.length != 2){
			throw new UDFArgumentException("array_to_string takes exactly 2 arguments");
		}
		if(args[0].getCategory() != ObjectInspector.Category.LIST || 
				(args[1].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) args[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING)){
			throw new UDFArgumentException("array_to_string takes as arguments (LIST, String)");
		}
		
		listInspector = (ListObjectInspector) args[0];
		stringInspector = (PrimitiveObjectInspector) args[1];
		
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	
	}


}
