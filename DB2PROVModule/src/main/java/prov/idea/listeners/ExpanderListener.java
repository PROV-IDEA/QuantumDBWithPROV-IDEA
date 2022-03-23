package prov.idea.listeners;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

//import org.mapdb.DB;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.interop.InteropFramework.ProvFormat;
import org.openprovenance.prov.model.Document;
import org.openprovenance.prov.model.Namespace;
import org.openprovenance.prov.model.ProvFactory;
import org.openprovenance.prov.model.QualifiedName;
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

public class ExpanderListener implements BGMEventListener {

//	Stopwatch insertClass;
//	Stopwatch insertSeq;
//	Stopwatch insertSM;

	ExecutorService exec = MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newFixedThreadPool(8),
			100_000, TimeUnit.DAYS// period after which executor will be automatically closed
									// I assume that 100_000 days is enough to simulate infinity
	);

	static HashMap<String, String> mapSMO = new HashMap<String, String>();
	static boolean tracedSmo = false;

	static TreeMap<String, Document> bufferBindings = new TreeMap<String, Document>();

	public static InteropFramework intFr = new InteropFramework();

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

	static boolean linkedControl = false;
	static int newColumnValueLinkedControl = 0, targetColumnValueLinkedControl = 0, targetColumnLinkedControl = 0;

	static TreeMap<String, Document> schemaTemplates = new TreeMap<String, Document>();
//	static TreeMap<String, Document> classTemplates = new TreeMap<String, Document>();
//	static TreeMap<String, Document> seqTemplates = new TreeMap<String, Document>();
//	static TreeMap<String, Document> stateTemplates = new TreeMap<String, Document>();

	static {

		ns.addKnownNamespaces();
		ns.register(EX_PREFIX, EX_NS);
		ns.register(VAR_PREFIX, VAR_NS);
		ns.register(TMPL_PREFIX, TMPL_NS);
		ns.register(XSD_PREFIX, XSD_NS);
		// Bea
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
		// templates refiere al directorio donde están los templates
		listf("templates", schemaTemplates);
		// listf("C:\\Users\\casaenad\\Dropbox\\Doctorado\\proyectos\\ATL\\workspace\\UML2PROV\\src-gen3\\templates\\sequence",seqTemplates);
//				listf("C:\\Users\\casaenad\\Dropbox\\Doctorado\\proyectos\\ATL\\workspace\\UML2PROV\\src-gen3\\templates\\state",stateTemplates);
		System.err.println("TODOS DENTRO");

	}

	ConcurrentHashMap<String, BindingPROVIDEA> methodBinding = new ConcurrentHashMap<String, BindingPROVIDEA>();
	TreeMap<String, String> statesMethod = new TreeMap<String, String>();
	private static final ProvFactory pf = InteropFramework.newXMLProvFactory();

	///// Iguales en los dos listeners
	@Override
	public void newValueBinding(BGMEvent e) {
		// System.out.println("Entra en newValueBinding con " + e.getClassName() + " y "
		// + e.getExecutionIdMethod());
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
				
		} else if (linkedControl && ( e.getVarName().compareTo("sourceColumnValue") == 0
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
//
//		BindingUML2PROV b = methodBinding.get(e.getExecutionIdMethod());
//		if(b==null) {
//			System.err.println("binding null for variable "+e.getExecutionIdMethod());
//		}else{
//			if (e.getVarName().compareTo("operationStartTime") == 0
//					|| e.getVarName().compareTo("operationEndTime") == 0
//					|| e.getVarName().compareTo("sourceState") == 0
//					|| e.getVarName().compareTo("targetState") == 0) {
//				b.addBinding("value", e);
//			} else {
//				b.addBinding("binding", e);
//			}
//		}
//		
////		if(e.getVarName().compareTo("operationStartTime")==0 
////				|| e.getVarName().compareTo("operationEndTime")==0
////				|| e.getVarName().compareTo("sourceState")==0
////				|| e.getVarName().compareTo("targetState")==0){
////			newValueBinding(e);
////		}else{
////			Bindings b = methodBinding.get(e.getExecutionIdMethod());
//////			if(b==null)System.err.println("binding null for variable "+e.getVarName()+" in "+e.getExecutionIdMethod());
//////			else System.err.println("----------->binding add variable "+e.getVarName()+" in "+e.getExecutionIdMethod());
////			if(b!=null) b.addVariable(var(e.getVarName()),ex(e.getValue()));
////		}
//		
//	}

	///// Iguales en los dos listeners
	@Override
	public void newBinding(BGMEvent e) {
		// Bea System.out.println("Entra en newBinding con " + e.getClassName() + " y "
		// + e.getExecutionIdMethod());
		BindingPROVIDEA b = methodBinding.get(e.getExecutionIdMethod());
		if (b == null) {
			System.err.println("binding null for variable " + e.getExecutionIdMethod());
		} else {
			System.out.println("e.getVarName() es " + e.getVarName());
			// value se invoca a addAttribute
			//////////////////////////////////////////
			//////////////////////////////////////////
			// no estoy segura de que haga falta este condicional (solo lo del else)
			////////////////// Insert
			if (e.getVarName().endsWith("EndTransTime")
					|| e.getVarName().endsWith("StartTransTime") 
			/////////// Copy
			) {
				////Nunca entra
				b.addBinding("value", e);

				// binding se invoca a addVariable
			} // Variables y variables con linked elements
			else {
				linkedControl = false;
				System.out.println("Entra ");
				if (e.getVarName().compareTo("newColumnValue") == 0
						|| e.getVarName().compareTo("targetColumnValue") == 0
						|| e.getVarName().compareTo("targetColumn") == 0) {
					System.out.println("%%%%%%%%%%%%%%%%%" + e.getVarName());
					linkedControl = true;
				} else {
					linkedControl = false;
				}
				b.addBinding("binding", e);
			}
		}

	}

	///// Iguales en los dos listeners
	@Override
	public void operationStart(BGMEvent e) {
		BindingPROVIDEA b = new BindingPROVIDEA(e.getExecutionIdMethod());
		// Bindings b = new Bindings(pf);
		methodBinding.put(e.getExecutionIdMethod(), b);
		statesMethod.put(e.getExecutionIdMethod(), e.getState());
//		System.err.println("start "+e.getExecutionIdMethod());
	}

	///// Iguales en los dos listeners SALVO la invocación a findAndExpand que le
	///// paso el schemaTemplates
	@Override
	public void operationEnd(BGMEvent eventEnd) {
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
						List<org.openprovenance.prov.model.TypedValue> listTypedValues = new ArrayList<org.openprovenance.prov.model.TypedValue>();
						
						TypedValue tv = new TypedValue();
						System.out.println("tttttt "+e.getVarName());
						if (e.getVarName().endsWith("StartTransTime") 
								|| e.getVarName().endsWith("EndTransTime")) {
							// Bea SimpleDateFormat xsdDateTime = new
							// SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
							// Bea Date d = new Date();
							// Bea d.setTime(Long.valueOf(e.getValue()));
							if (e.getVarName().endsWith("EndTransTime"))
								System.out.println("es endTransTime");
							tv.setType(pf.getName().XSD_DATETIME);
							// Bea tv.setValue(xsdDateTime.format(d));
							tv.setValue(e.getValue());
						} else {
							// if(!e.getValue().equalsIgnoreCase("Paula")) {
							
							tv.setType(pf.getName().XSD_STRING);
							tv.setValue(e.getValue());
//							}
//							else {
//								System.out.println("atributo55555 asocia "+ e.getVarName() + " con "+ e.getValue()+"--");
//								
//							}
						}
						listTypedValues.add(tv);

						if (b == null) {
							System.err.println(
									"binding null for variable " + e.getVarName() + " in " + e.getExecutionIdMethod());
						} else {
							b.addAttribute(var(e.getVarName()), listTypedValues);
							System.out.println("atributo asocia " + e.getVarName() + " con " + e.getValue());
						}

						/**/
					} else if (eventPair.getKey().compareTo("binding") == 0) {
						if (b != null) {
//							if (e.getVarName().equalsIgnoreCase("SMO")) {
//								tracedSmo= true;
//								mapSMO.put()
//							}
							b.addVariable(var(e.getVarName()), ex(e.getValue()));
							System.out.println("variable asocia " + e.getVarName() + " con " + e.getValue());
						}

						
					}
				}

				// Bea String nameTemplate =
				// eventEnd.getClassName()+"_"+eventEnd.getExecutionIdMethod().split("-")[1];
				String nameTemplate = eventEnd.getClassName() + "PROVTemplate";
				System.out.println("nameTemplate es " + nameTemplate);

				try {
					// Bea insertClass.elapsedTime();
					String operatorType = eventEnd.getClassName();
					findAndExpand(eventEnd.getExecutionIdMethod(), nameTemplate, col, b, schemaTemplates, operatorType);
					// Bea findAndExpand(eventEnd.getExecutionIdMethod(),nameTemplate, col, b,
					// classTemplates,"CLASS");
					// Bea findAndExpand(eventEnd.getExecutionIdMethod(),nameTemplate, col, b,
					// seqTemplates,"SEQUENCE");
//					findAndExpand(eventEnd.getExecutionIdMethod(),nameTemplate+"_"+sourceState+"-"+targetState, col, b, stateTemplates, "STATE");
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}

/////Iguales en los dos listeners
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

/////Iguales en los dos listeners
	public static QualifiedName var(String param) {
		return ns.qualifiedName(VAR_PREFIX, param, pf);
	}

//////Añadidos por Bea
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

//	private synchronized void findAndExpand(String idBinding, String nameTemplate, 
//			DBCollection coll, final Bindings binding, TreeMap<String, Document> classTemplates, String UMLDiagram)	
//					throws FileNotFoundException {
	/// Lo he tenido que adaptar (tanto cabecera como lo que mete a obj
	private synchronized void findAndExpand(String idBinding, String nameTemplate, DBCollection coll,
			final Bindings binding, TreeMap<String, Document> classTemplates, String operationType)
			throws FileNotFoundException {
		// Bea Stopwatch thisMethod = new Stopwatch();
		Document docTemplate = classTemplates.get(nameTemplate);
		// System.out.println("docTemplate"+docTemplate.toString());
		URL directoryURL = ExpanderListener.class.getResource("..");
		new File(directoryURL + "expanded").mkdir();

		if (docTemplate != null) {

			Document expanded = expander(docTemplate, binding);
			ByteArrayOutputStream bout = new ByteArrayOutputStream();

			intFr.writeDocument(bout, ProvFormat.JSON, expanded);

			/*
			 * DBObject obj = BasicDBObjectBuilder.start().get(); obj.put("_id", idBinding +
			 * "_" + UMLDiagram); obj.put("class", idBinding.split("-")[0]);
			 * obj.put("method", idBinding.split("-")[1]); obj.put("UMLDiagram",
			 * UMLDiagram); obj.put("expandedDocument", bout.toString()); col.insert(obj);
			 */

			DBObject obj = BasicDBObjectBuilder.start().get();
			obj.put("_id", idBinding + "_" + operationType);
			obj.put("class", idBinding.split("-")[0]);
			obj.put("method", idBinding.split("-")[1]);
			obj.put("operationType", operationType);
			obj.put("expandedDocument", bout.toString());
			col.insert(obj);

		} else {
			System.out.println("docTemplate es null");
		}
	}

	private synchronized Document expander(Document template, final Bindings bindings) {
		Document expanded = null;
		Expand e = new Expand(pf, false, true);
		expanded = e.expander(template, bindings);
		return expanded;
	}

	private static void listf(String directoryName, TreeMap<String, Document> templates) {

		// URL directoryURL = ListenerMongoDB_expanded.class.getResource("templates");
		URL directoryURL = ExpanderListener.class.getResource("..");
		System.out.println("path " + directoryURL.getPath());
		File directory = new File(directoryURL.getPath() + directoryName);// directoryURL.toString());
		// File directory = new File("./");
		// System.out.println(directory.getAbsolutePath());

		if (directory.listFiles() != null) {

			// get all the files from a directory
			File[] fList = directory.listFiles();

			for (File file : fList) {
				// Bea if (file.isFile()) {
				System.out.println("nombre fichero " + file.getName());
				Document d = intFr.readDocumentFromFile(file.getAbsolutePath(), ProvFormat.JSON);
				templates.put(file.getName().split("\\.")[0], d);
				// Bea} else if (file.isDirectory()) {
				// Bealistf(file.getAbsolutePath(), templates);
				// Bea}
			}
		}
	}

}
