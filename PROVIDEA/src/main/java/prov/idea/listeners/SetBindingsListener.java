package prov.idea.listeners;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.interop.InteropFramework.ProvFormat;
import org.openprovenance.prov.model.Document;
import org.openprovenance.prov.model.Namespace;
import org.openprovenance.prov.model.ProvFactory;
import org.openprovenance.prov.model.QualifiedName;
import org.openprovenance.prov.model.Statement;
import org.openprovenance.prov.model.exception.QualifiedNameException;
import org.openprovenance.prov.template.Bindings;
import org.openprovenance.prov.template.Expand;
import org.openprovenance.prov.xml.TypedValue;

import com.google.common.util.concurrent.MoreExecutors;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import prov.idea.listeners.BindingPROVIDEA;
import prov.idea.events.BGMEvent;

//import aspects.Stopwatch;

public class SetBindingsListener implements BGMEventListener {

//	Stopwatch insertClass;
//	Stopwatch insertSeq;
//	Stopwatch insertSM;

	ExecutorService exec = MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newFixedThreadPool(8),
			100_000, TimeUnit.DAYS// period after which executor will be automatically closed
									// I assume that 100_000 days is enough to simulate infinity
	);

	static TreeMap<String, Document> bufferBindings = new TreeMap<String, Document>();
	
	static boolean linkedControl = false;
	static int newColumnValueLinkedControl = 0, targetColumnValueLinkedControl = 0, targetColumnLinkedControl = 0;

	public static Namespace ns = new Namespace();
	public static final String EX_NS = "http://example.org/";
	public static final String EX_PREFIX = "exe";
	public static final String VAR_PREFIX = "var";
	public static final String VAR_NS = "http://openprovenance.org/var#";
	public static final String TMPL_PREFIX = "tmpl";
	public static final String TMPL_NS = "http://openprovenance.org/tmpl#";
	public static final String XSD_PREFIX = "xsd";
	public static final String XSD_NS = "http://www.w3.org/2001/XMLSchema#";
	// Pregunta para Carlos: por qué no hay ningún prefijo de los de uml2prov?
	// Bea
	public static final String O2P_PREFIX = "o2p";
	public static final String O2P_NS = "http://uml2prov.unirioja.es/ns/o2p#";
	public static final String SCH2P_PREFIX = "sch2p";
	public static final String SCH2P_NS = "http://uml2prov.unirioja.es/ns/sch2p#";
	public static final String D2P_PREFIX = "d2p";
	public static final String D2P_NS = "http://uml2prov.unirioja.es/ns/d2p#";
	public static final String BITEMP_PREFIX = "bitemp";
	public static final String BITEMP_NS = "http://uml2prov.unirioja.es/ns/bitemp#";
	//////////////////////////////////////////////////////////////////////////////////
	private static HashMap<String, Bindings> listBindings = new HashMap<String, Bindings>();

	static Mongo mongo;
	static DB db;
	static DBCollection col;
	static BasicDBObjectBuilder docBuilder;

//Bea: Supuestamente, no se usan templates, los comento para limpiar
//	static TreeMap<String, Document> classTemplates = new TreeMap<String, Document>();
//	static TreeMap<String, Document> seqTemplates = new TreeMap<String, Document>();
//	static TreeMap<String, Document> stateTemplates = new TreeMap<String, Document>();

	static {

		ns.addKnownNamespaces();
		ns.register(EX_PREFIX, EX_NS);
		ns.register(VAR_PREFIX, VAR_NS);
		ns.register(TMPL_PREFIX, TMPL_NS);
		ns.register(XSD_PREFIX, XSD_NS);
		//Bea
		ns.register(O2P_PREFIX, O2P_NS);
		ns.register(SCH2P_PREFIX, SCH2P_NS);
		ns.register(D2P_PREFIX, D2P_NS);
		ns.register(BITEMP_PREFIX, BITEMP_NS);

		try {
			mongo = new Mongo("127.0.0.1", 27017);
			db = mongo.getDB("PROVDISEA");
			col = db.getCollection("Schema_Changes");
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

//				listf("C:\\Users\\casaenad\\Dropbox\\Doctorado\\proyectos\\ATL\\workspace\\UML2PROV\\src-gen3\\templates\\class",classTemplates);
//				listf("C:\\Users\\casaenad\\Dropbox\\Doctorado\\proyectos\\ATL\\workspace\\UML2PROV\\src-gen3\\templates\\sequence",seqTemplates);
//				listf("C:\\Users\\casaenad\\Dropbox\\Doctorado\\proyectos\\ATL\\workspace\\UML2PROV\\src-gen3\\templates\\state",stateTemplates);
//				System.err.println("TODOS DENTRO");

	}

	ConcurrentHashMap<String, BindingPROVIDEA> methodBinding = new ConcurrentHashMap<String, BindingPROVIDEA>();
	TreeMap<String, String> statesMethod = new TreeMap<String, String>();
	private static final ProvFactory pf = InteropFramework.newXMLProvFactory();
	public static InteropFramework intFr = new InteropFramework();

	@Override
	public void newValueBinding(BGMEvent e) {
		System.out.println("Entra en newValueBinding con " + e.getClassName() + " y " + e.getExecutionIdMethod());
		BindingPROVIDEA b = methodBinding.get(e.getExecutionIdMethod());
		if (b == null) {
			System.err.println("binding null for variable " + e.getVarName() + " in " + e.getExecutionIdMethod());
		} else {

			/// Daba la mala casualidad de que, tras recorrer todo lo de "newColumnValue",
			// incluidos sus linked, la siguiente variable era "value" y, al ser
			/// linkedControl true, entraba
			// aquí y lo ponía como value en lugar de binding.... por eso se me ocurrió
			/// "contar" los linked de cada variable
			// que los tenga y, de llegar al número, linkedControl pasaría a false
			if (linkedControl
					&& (e.getVarName().compareTo("column") == 0 || e.getVarName().compareTo("value") == 0)) {

				newColumnValueLinkedControl++;
				System.out.println("es "+newColumnValueLinkedControl);
				if (newColumnValueLinkedControl == 2) {
					linkedControl = false;
					newColumnValueLinkedControl = 0;
				}
				
			} else if (linkedControl && (e.getVarName().compareTo("sourColumn") == 0)){
					linkedControl = false;
					
			} else if (linkedControl && (e.getVarName().compareTo("inputColumnDef") == 0)){
				linkedControl = false;
				
		}else if (linkedControl && ( e.getVarName().compareTo("sourceColumnValue") == 0
					|| e.getVarName().compareTo("targetColumn") == 0)) {
				targetColumnValueLinkedControl++;
				if (targetColumnValueLinkedControl == 2) {
					linkedControl = false;
					targetColumnValueLinkedControl = 0;
				}
				
			}
			
			
			b.addBinding("value", e);
		}

//		
//		List<org.openprovenance.prov.model.TypedValue> listTypedValues = new ArrayList<org.openprovenance.prov.model.TypedValue>();
//		
//		TypedValue tv = new TypedValue();	
//		if(e.getVarName().compareTo("operationStartTime")==0 || e.getVarName().compareTo("operationEndTime")==0){
//			SimpleDateFormat xsdDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
//			Date d = new Date();
//			d.setTime(Long.valueOf(e.getValue()));
//			tv.setType(pf.getName().XSD_DATETIME);
//			tv.setValue(xsdDateTime.format(d));
//		}else{
//			tv.setType(pf.getName().XSD_STRING);
//			tv.setValue(e.getValue());
//		}
//		listTypedValues.add(tv);
//		
//		Bindings b = methodBinding.get(e.getExecutionIdMethod());
//		if(b==null) {
//			System.err.println("binding null for variable "+e.getVarName()+" in "+e.getExecutionIdMethod());
//		}else{
//			b.addAttribute(var(e.getVarName()), listTypedValues);
//		}

	}

//	@Override
//	public void newBinding(BGMEvent e) {
//		System.out.println("Entra en newBinding con "+ e.getClassName() + " y "+ e.getExecutionIdMethod());
//		BindingPROVIDEA b = methodBinding.get(e.getExecutionIdMethod());
//		if(b==null) {
//			System.err.println("binding null for variable "+e.getExecutionIdMethod());
//		}else{
//			//value se invoca a addAttribute
//			if (e.getVarName().compareTo("operationStartTime") == 0
//					|| e.getVarName().compareTo("operationEndTime") == 0
//					|| e.getVarName().compareTo("sourceState") == 0
//					|| e.getVarName().compareTo("targetState") == 0) {
//				b.addBinding("value", e);
//			//binding se invoca a addVariable
//			} else {
//				b.addBinding("binding", e);
//			}
//		}
//	}

	@Override
	public void newBinding(BGMEvent e) {
		// Bea System.out.println("Entra en newBinding con " + e.getClassName() + " y " + e.getExecutionIdMethod());
		BindingPROVIDEA b = methodBinding.get(e.getExecutionIdMethod());
		if (b == null) {
			System.err.println("binding null for variable " + e.getExecutionIdMethod());
		} else {
			// value se invoca a addAttribute
			//////////////////Insert
			if (e.getVarName().compareTo("operatorName") == 0 
					|| e.getVarName().endsWith("EndTransTime") 
					|| e.getVarName().endsWith("StartTransTime")  
					|| e.getVarName().compareTo("executed") == 0
					|| e.getVarName().compareTo("operationInstruction") == 0
					|| e.getVarName().compareTo("tableName") == 0
					|| e.getVarName().compareTo("inputValue") == 0
					|| e.getVarName().compareTo("columnName") == 0
					|| e.getVarName().compareTo("columnValue") == 0
					|| e.getVarName().compareTo("rowId") == 0
					////////////Create///////////////////
					|| e.getVarName().compareTo("columnType") == 0
					|| e.getVarName().compareTo("schemaName") == 0
					///////////Copy
				)
			{
				b.addBinding("value", e);
				// binding se invoca a addVariable
			} else {
				linkedControl = false;
				System.out.println("Entra ");
				System.out.println("%%%%%%%%%%%%%%%%%"+ e.getVarName());
				if (e.getVarName().compareTo("newColumnValue") == 0
						|| e.getVarName().compareTo("targetColumnValue") == 0
						|| e.getVarName().compareTo("targetColumn") == 0) {
					
					linkedControl = true;
				} else {
					linkedControl = false;
				}
				b.addBinding("binding", e);
			}
		}

//		if(e.getVarName().compareTo("operationStartTime")==0 
//				|| e.getVarName().compareTo("operationEndTime")==0
//				|| e.getVarName().compareTo("sourceState")==0
//				|| e.getVarName().compareTo("targetState")==0){
//			newValueBinding(e);
//		}else{
//			Bindings b = methodBinding.get(e.getExecutionIdMethod());
////			if(b==null)System.err.println("binding null for variable "+e.getVarName()+" in "+e.getExecutionIdMethod());
////			else System.err.println("----------->binding add variable "+e.getVarName()+" in "+e.getExecutionIdMethod());
//			if(b!=null) b.addVariable(var(e.getVarName()),ex(e.getValue()));
//		}

	}

	@Override
	public void operationStart(BGMEvent e) {
		//Bea System.out.println("Entra en operationStart con " + e.getClassName() + " y " + e.getExecutionIdMethod());
		BindingPROVIDEA b = new BindingPROVIDEA(e.getExecutionIdMethod());
		// Bindings b = new Bindings(pf);
		//Bea System.out.println("mete en methodBinding " + e.getExecutionIdMethod() + " y " + b);
		methodBinding.put(e.getExecutionIdMethod(), b);
		statesMethod.put(e.getExecutionIdMethod(), e.getState());
//		System.err.println("start "+e.getExecutionIdMethod());
	}

	@Override
	public void operationEnd(BGMEvent eventEnd) {
		System.out.println(
				"Entra en operationEnd con " + eventEnd.getClassName() + " y " + eventEnd.getExecutionIdMethod());
		// Bea: no se usaba String sourceState =
		// statesMethod.get(eventEnd.getExecutionIdMethod());
		// Bea: no se usaba String targetState = eventEnd.getState();

		exec.execute(() -> {
			// Bea insertClass = new Stopwatch();

			BindingPROVIDEA binding = methodBinding.get(eventEnd.getExecutionIdMethod());
			if (binding != null) {
				Bindings b = new Bindings(pf);

				for (Entry<String, BGMEvent> eventPair : binding.getBindings()) {
					BGMEvent e = eventPair.getValue();

					/**/
					if (eventPair.getKey().compareTo("value") == 0) {
						 System.out.println("entra a value con b="+e.getVarName()+ " "+e.getValue());
						List<org.openprovenance.prov.model.TypedValue> listTypedValues = new ArrayList<org.openprovenance.prov.model.TypedValue>();

						TypedValue tv = new TypedValue();
						if (e.getVarName().endsWith("StartTransTime") 
								|| e.getVarName().endsWith("EndTransTime") ) {
							//Bea SimpleDateFormat xsdDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
							//Bea Date d = new Date();
							//Bea d.setTime(Long.valueOf(e.getValue()));
							
							tv.setType(pf.getName().XSD_DATETIME);
							//Bea tv.setValue(xsdDateTime.format(d));
							tv.setValue(e.getValue());
						} else {
							tv.setType(pf.getName().XSD_STRING);
							tv.setValue(e.getValue());
						}
						listTypedValues.add(tv);

						if (b == null) {
							System.err.println(
									"binding null for variable " + e.getVarName() + " in " + e.getExecutionIdMethod());
						} else {
							b.addAttribute(var(e.getVarName()), listTypedValues);
//							List<Statement> ll=new ArrayList();
//							b.add2Dvalues(ll,
//						              new Hashtable<QualifiedName,List<List<TypedValue>>>().put(var(e.getVarName()),listTypedValues));
						}

						/**/
					} else if (eventPair.getKey().compareTo("binding") == 0) {
						 System.out.println("entra a binding con b="+e.getVarName()+ " "+e.getValue());
						if (b != null)
							b.addVariable(var(e.getVarName()), ex(e.getValue()));
					}
				}

				String nameTemplate = eventEnd.getClassName() + "_" + eventEnd.getExecutionIdMethod().split("-")[1];

				try {
					// Bea insertClass.elapsedTime();
					// Bea: cambié la declaración del método porque los templates no se usan
					// Bea (Carlos tenía comentados los otros dos)
					// findAndExpand(eventEnd.getExecutionIdMethod(),nameTemplate, col, b,
					// classTemplates,"CLASS");
					String operatorType = eventEnd.getClassName();
					findAndExpand(eventEnd.getExecutionIdMethod(), nameTemplate, col, b, operatorType);
//					findAndExpand(eventEnd.getExecutionIdMethod(),nameTemplate, col, b, seqTemplates,"SEQUENCE");
//					findAndExpand(eventEnd.getExecutionIdMethod(),nameTemplate+"_"+sourceState+"-"+targetState, col, b, stateTemplates, "STATE");
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}

	public static QualifiedName ex(String param) {
		QualifiedName rtn = null;

		try {
			rtn = ns.qualifiedName(EX_PREFIX, param.replace("[]", ""), pf);
		} catch (QualifiedNameException e) {
//			System.err.println("QualifiedNameException");
//			System.err.println("param: "+ param);
		}
		return rtn;

	}

	public static QualifiedName var(String param) {
		return ns.qualifiedName(VAR_PREFIX, param, pf);
	}

	////// Añadidos por Bea
	//////////////////////////////////////////////////////////////
	public static QualifiedName o2p(String param) {
		return ns.qualifiedName(O2P_PREFIX, param, pf);
	}

	public static QualifiedName sch2p(String param) {
		return ns.qualifiedName(SCH2P_PREFIX, param, pf);
	}

	public static QualifiedName d2p(String param) {
		return ns.qualifiedName(D2P_PREFIX, param, pf);
	}

	public static QualifiedName bitemp(String param) {
		return ns.qualifiedName(BITEMP_PREFIX, param, pf);
	}
	/////////////////////////////////////////////////////////////

	// Bea: quité el parámetro de templates porque en este listening no se usa
	// private synchronized void findAndExpand(String idBinding, String
	// nameTemplate, DBCollection coll, final Bindings binding, TreeMap<String,
	// Document> classTemplates, String OperatorType) throws FileNotFoundException{
	private synchronized void findAndExpand(String idBinding, String nameTemplate, DBCollection coll,
			final Bindings binding, String OperatorType) throws FileNotFoundException {
		// System.out.println("Entra a findAndExpand con binding "+ binding);

		// Stopwatch thisMethod = new Stopwatch();
//		Document docTemplate = classTemplates.get(nameTemplate);
//		
//		new File("expanded").mkdir();
//
//		if (docTemplate != null) {
//
//			Document expanded = expander(docTemplate, binding);
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		// Bea System.out.println("Entra a findAndExpand con intFr " + intFr);
		// ProvFormat.TURTLE
		intFr.writeDocument(bout, ProvFormat.JSON, binding.toDocument());
//
		DBObject obj = BasicDBObjectBuilder.start().get();
		obj.put("_id", idBinding + "_" + OperatorType);
		obj.put("setBindings", bout.toString());
//			obj.put("method", idBinding.split("-")[1]);
//			obj.put("UMLDiagram", UMLDiagram);
//			obj.put("expandedDocument", bout.toString());
//			obj.put("time", thisMethod.elapsedTime()+insertClass.getElapseTime());
		col.insert(obj);
//
//		}
	}

	private synchronized Document expander(Document template, final Bindings bindings) {
		Document expanded = null;
		Expand e = new Expand(pf, false, true);
		expanded = e.expander(template, bindings);
		return expanded;
	}

	private static void listf(String directoryName, TreeMap<String, Document> templates) {

		File directory = new File(directoryName);

		if (directory.listFiles() != null) {

			// get all the files from a directory
			File[] fList = directory.listFiles();

			for (File file : fList) {
				if (file.isFile()) {
					Document d = intFr.readDocumentFromFile(file.getAbsolutePath(), ProvFormat.JSON);
					templates.put(file.getName().split("\\.")[0], d);
				} else if (file.isDirectory()) {
					listf(file.getAbsolutePath(), templates);
				}
			}
		}
	}

//	static Mongo mongo;
//	static DB db;
//	static DBCollection col;
//	static BasicDBObjectBuilder docBuilder;
//	
//	static {
//		try {
//			mongo = new Mongo("127.0.0.1", 27017);
//			db = mongo.getDB("provenance");
//			col = db.getCollection("script2_bindings");
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (MongoException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//
//	@Override
//	public void newValueBinding(BGMEvent e){
//
//			BasicDBObjectBuilder docBuilder = BasicDBObjectBuilder.start();
//			
//			 DBObject obj = docBuilder.get();
//				
//			 obj.put("Execution_ID", e.getExecutionID());
//			 obj.put("Identifier", e.getIdentifier());
//			 obj.put("type", e.getType());
//			 obj.put("VALUE", e.getValue());
//			 
//			 col.insert(obj);
//	}
//	
//	@Override
//	public void newBinding(BGMEvent e) {
//			 BasicDBObjectBuilder docBuilder = BasicDBObjectBuilder.start();
//			 DBObject obj = docBuilder.get();
//
//				
//			 obj.put("Execution_ID", e.getExecutionID());
//			 obj.put("Class", e.getClassName());
//			 obj.put("Execution_ID_METHOD", e.getExecutionIdMethod());
//			 obj.put("VARIABLE", e.getVarName());
//			 obj.put("VALUE", e.getValue());
////			 System.out.println(obj.toString().replace(".", "").replace("\"", "'"));
////			 cacheBindings.add(obj);
////			 Stopwatch sw = new Stopwatch();
////			 LOGGER.debug(obj.toString().replace("", "").replace("\"", "'"));
//			 
//			 col.insert(obj);
//	}
//
//	@Override
//	public void operationStart(BGMEvent e) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void operationEnd(BGMEvent e) {
//		// TODO Auto-generated method stub
//		
//	}

}
