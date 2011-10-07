import intencode.CategoryDecode;
import intencode.CategoryEncode;
import junit.framework.TestCase;


public class EncodeDecodeTest extends TestCase {

	public void testSimpeEncoding(){
		String cat = "MLA01234";
		String encode =new CategoryEncode().evaluate(cat);
		assertTrue("Encode int Ok",Integer.parseInt(encode) == 1001234);
		assertTrue("Encode string Ok",encode.equals("1001234"));
		String decode = new CategoryDecode().evaluate(encode);
		assertTrue("Decode Ok", cat.equals(decode));
	}
}
