package intencode;

public enum SiteEnum {
	
	MLA(10),MLB(11),MCO(12),MCR(13),MEC(14),MLC(15),MLM(16),MLU(17),MLV(18),MPA(19),MPE(20),MPT(21),MRD(22),ODR(23);
	
	private int code;

	SiteEnum(int c){
		code = c;
	}
	
	public static SiteEnum getSiteEnum(String str){
		try{
			return SiteEnum.valueOf(str.toUpperCase());
		}catch(IllegalArgumentException e){
			return SiteEnum.ODR;
		}
	}
	
	public static SiteEnum getSiteEnum(int id){
		try{
			return values()[id-10];
		}catch(Throwable e){
			throw new RuntimeException("Error decoding id["+id+"]",e);
		}
	}
	
	public int getCode() {
		return code;
	}
	
	
}
