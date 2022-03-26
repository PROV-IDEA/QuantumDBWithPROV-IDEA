package prov.idea.instrumenter;


import java.util.Random;

public class UUID {
	static private final String uuidIdentifier = System.getProperty("user.name");
	static private Long identifier1 = new Long(0); //new Long(Long.MIN_VALUE);
	static private Integer identifier2 = Math.abs(new Random().nextInt());
	
	private String clss;
	private String uuid;
	private Integer counter;
	
	private final static Random STARTER = new Random();

	public UUID(Object obj) {
		this.clss = (obj.getClass().getSimpleName().isEmpty())?"noClass":obj.getClass().getSimpleName();
		this.counter = 1;
		this.uuid = randomUUID();

	}

	public String incrementUUID() {
		this.counter++;
		return this.getCompleteUUID();
	}

	public String getCompleteUUID() {
		return this.clss + "-" + this.uuid + "_" + this.counter;
	}

	public String getUUID() {
		return this.clss + "-" + this.uuid;
	}

	public synchronized static String randomUUID() {
		if(identifier1==Integer.MAX_VALUE){
			identifier1=Long.MIN_VALUE;
			identifier2++;
		}else{
			identifier1++;
		}
		return String.valueOf(identifier2)+String.valueOf(identifier1);
	}

	public String getClss() {
		// TODO Auto-generated method stub
		return this.clss;
	}	
}


