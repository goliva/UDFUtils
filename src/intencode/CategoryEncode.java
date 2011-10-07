package intencode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class CategoryEncode extends UDF{
	
	Pattern p = Pattern.compile("([A-Z]{3})(.*)");
	Text res = new Text();
	
	public Text evaluate(final Text s){
		if(s==null)
			return null;
		res.clear();
		StringBuilder sb = new  StringBuilder();
		Matcher m = p.matcher(s.toString());
		if(m.find()){
			int code = SiteEnum.getSiteEnum(m.group(1)).getCode();
			if(code<10)
				sb.append('0');
			sb.append(code);
			sb.append(m.group(2));
			res.set(sb.toString());
		}else{
			res.set("");
		}
		return res;
	}
	
	public String evaluate(final String s){
		if(s==null)
			return null;
		return evaluate(new Text(s)).toString();
	}
}
