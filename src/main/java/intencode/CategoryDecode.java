package intencode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class CategoryDecode extends UDF {
	Pattern p = Pattern.compile("(\\d{2})(.*)");
	Text res = new Text();
	
	public String evaluate(final String str) {
		if(str==null)
			return null;
		return evaluate(new Text(str)).toString();
	}

	public Text evaluate(final Text t) {
		if(t==null)
			return null;
		res.clear();
		Matcher m = p.matcher(t.toString());
		StringBuffer sb = new StringBuffer();
		if(m.find()){
			sb.append(SiteEnum.getSiteEnum(Integer.parseInt(m.group(1))).toString());
			sb.append(m.group(2));
			res.set(sb.toString());
		}else{
			res.set("");
		}
		return res;
	}

}
