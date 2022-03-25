package prov.idea.instrumenter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class IdentifiersManager implements Identified {
	public  UUID uuid = null;
	
	private static Set<Class> WRAPPER_TYPES = new HashSet(Arrays.asList(String.class, Boolean.class, Character.class,
			Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class, Void.class));

	private static boolean isWrapperType(Class clazz) {
		return WRAPPER_TYPES.contains(clazz);
	}

	static String incrementIdentifier(Object obj) {
		String rslt;
		if (!Identified.class.isAssignableFrom(obj.getClass())) {
			rslt = new UUID(obj).incrementUUID();
		} else {
			rslt = ((Identified) obj).getUUID().incrementUUID();
		}
		return rslt;
	}

	static UUID getIdentifier(Object obj) {
		UUID rtn = null;

		if (Identified.class.isAssignableFrom(obj.getClass())) {
			if (((Identified) obj).getUUID() == null)
				((Identified) obj).setUUID(new UUID(obj));
			rtn = ((Identified) obj).getUUID();
		} else {
			rtn = new UUID(obj);
		}
		return rtn;
	}

	static synchronized public String randomUUID() {
		return UUID.randomUUID();
	}

	static public UUID newUUID(Object obj) {
		UUID uuid = new UUID(obj);
		return uuid;
	}

	
	
	@Override
	public void setUUID(UUID newUuid) {
		// TODO Auto-generated method stub
		System.out.println("invoca al set");
		
			uuid = newUuid;
		
	
	}

	@Override
	public UUID getUUID() {
		// TODO Auto-generated method stub
		return uuid;
	}

	


}