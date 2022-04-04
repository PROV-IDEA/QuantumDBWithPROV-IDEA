package prov.idea.listeners;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.openprovenance.prov.interop.InteropException;
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
		// Bea
		ns.register(O2P_PREFIX, O2P_NS);
		ns.register(SCH2P_PREFIX, SCH2P_NS);
		ns.register(D2P_PREFIX, D2P_NS);
		ns.register(BITEMP_PREFIX, BITEMP_NS);

		try {
			mongo = new Mongo("127.0.0.1", 27017);
			db = mongo.getDB("PROVIDEA");
			col = db.getCollection("Schema_Changes");
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	ConcurrentHashMap<String, BindingPROVIDEA> methodBinding = new ConcurrentHashMap<String, BindingPROVIDEA>();
	TreeMap<String, String> statesMethod = new TreeMap<String, String>();
	private static final ProvFactory pf = InteropFramework.newXMLProvFactory();
	public static InteropFramework intFr = new InteropFramework();

	@Override
	public void newValueBinding(BGMEvent e) {

		BindingPROVIDEA b = methodBinding.get(e.getExecutionIdMethod());
		if (b == null) {
			System.err.println("binding null for variable " + e.getVarName() + " in " + e.getExecutionIdMethod());
		} else {

			if (linkedControl && (e.getVarName().compareTo("column") == 0 || e.getVarName().compareTo("value") == 0)) {

				newColumnValueLinkedControl++;
				
				if (newColumnValueLinkedControl == 2) {
					linkedControl = false;
					newColumnValueLinkedControl = 0;
				}

			} else if (linkedControl && (e.getVarName().compareTo("sourColumn") == 0)) {
				linkedControl = false;

			} else if (linkedControl && (e.getVarName().compareTo("inputColumnDef") == 0)) {
				linkedControl = false;

			} else if (linkedControl && (e.getVarName().compareTo("sourceColumnValue") == 0
					|| e.getVarName().compareTo("targetColumn") == 0)) {
				targetColumnValueLinkedControl++;
				if (targetColumnValueLinkedControl == 2) {
					linkedControl = false;
					targetColumnValueLinkedControl = 0;
				}

			}

			b.addBinding("value", e);
		}

	}

	@Override
	public void newBinding(BGMEvent e) {
		
		BindingPROVIDEA b = methodBinding.get(e.getExecutionIdMethod());
		if (b == null) {
			System.err.println("binding null for variable " + e.getExecutionIdMethod());
		} else {

			if (e.getVarName().compareTo("operatorName") == 0 || e.getVarName().endsWith("EndTransTime")
					|| e.getVarName().endsWith("StartTransTime") || e.getVarName().compareTo("executed") == 0
					|| e.getVarName().compareTo("operationInstruction") == 0
					|| e.getVarName().compareTo("tableName") == 0 || e.getVarName().compareTo("inputValue") == 0
					|| e.getVarName().compareTo("columnName") == 0 || e.getVarName().compareTo("columnValue") == 0
					|| e.getVarName().compareTo("rowId") == 0

					|| e.getVarName().compareTo("columnType") == 0 || e.getVarName().compareTo("schemaName") == 0

			) {
				b.addBinding("value", e);

			} else {
				linkedControl = false;

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

	}

	@Override
	public void operationStart(BGMEvent e) {

		BindingPROVIDEA b = new BindingPROVIDEA(e.getExecutionIdMethod());

		methodBinding.put(e.getExecutionIdMethod(), b);
		statesMethod.put(e.getExecutionIdMethod(), e.getState());

	}

	@Override
	public void operationEnd(BGMEvent eventEnd) {

		exec.execute(() -> {

			BindingPROVIDEA binding = methodBinding.get(eventEnd.getExecutionIdMethod());
			if (binding != null) {
				Bindings b = new Bindings(pf);

				for (Entry<String, BGMEvent> eventPair : binding.getBindings()) {
					BGMEvent e = eventPair.getValue();

					/**/
					if (eventPair.getKey().compareTo("value") == 0) {
						List<org.openprovenance.prov.model.TypedValue> listTypedValues = new ArrayList<org.openprovenance.prov.model.TypedValue>();

						TypedValue tv = new TypedValue();
						if (e.getVarName().endsWith("StartTransTime") || e.getVarName().endsWith("EndTransTime")) {

							tv.setType(pf.getName().XSD_DATETIME);

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

						}

						/**/
					} else if (eventPair.getKey().compareTo("binding") == 0) {
						if (b != null)
							b.addVariable(var(e.getVarName()), ex(e.getValue()));
					}
				}

				String nameTemplate = eventEnd.getClassName() + "_" + eventEnd.getExecutionIdMethod().split("-")[1];

				try {

					String operatorType = eventEnd.getClassName();
					findAndExpand(eventEnd.getExecutionIdMethod(), nameTemplate, col, b, operatorType);

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
			if (param != null)
				rtn = ns.qualifiedName(EX_PREFIX, param.replace("[]", ""), pf);
		} catch (QualifiedNameException e) {

		}
		return rtn;

	}

	public static QualifiedName var(String param) {
		return ns.qualifiedName(VAR_PREFIX, param, pf);
	}

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

	private synchronized void findAndExpand(String idBinding, String nameTemplate, DBCollection coll,
			final Bindings binding, String OperatorType) throws FileNotFoundException {
		try {

			if (binding != null && intFr != null) {
				ByteArrayOutputStream bout = new ByteArrayOutputStream();

				Document d = binding.toDocument();
				if (d != null) {
					intFr.writeDocument(bout, ProvFormat.JSON, d);

					DBObject obj = BasicDBObjectBuilder.start().get();
					obj.put("_id", idBinding + "_" + OperatorType);
					obj.put("setBindings", bout.toString());

					col.insert(obj);
				}

			}
		} catch (NullPointerException e) {
			// e.printStackTrace();
		}

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

}
