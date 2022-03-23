package prov.idea.instrumenter;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.DeclareParents;
import org.aspectj.lang.annotation.Pointcut;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.quantumdb.core.backends.Config;
import io.quantumdb.core.migration.Migrator.Stage;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.ColumnDefinition;
import io.quantumdb.core.schema.operations.CopyTable;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.schema.operations.DataOperation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import prov.idea.events.BGMEvent;
import prov.idea.events.EventHelper;
import prov.idea.listeners.BGMEventListener;
import prov.idea.listeners.SetBindingsListener;

@Aspect
public class BGMEventInstrumenter {
	// ,CopyTableMigrator,DataOperation,DatabaseMigrator
	@DeclareParents(value = "io.quantumdb.core.versioning.ChangeSet", defaultImpl = IdentifiersManager.class)
	public static Identified ident1;
	@DeclareParents(value = "io.quantumdb.core.migration.operations.CreateTableMigrator", defaultImpl = IdentifiersManager.class)
	public static Identified ident2;
	@DeclareParents(value = "io.quantumdb.core.migration.operations.CopyTableMigrator", defaultImpl = IdentifiersManager.class)
	public static Identified ident3;
	@DeclareParents(value = "io.quantumdb.core.schema.operations.DataOperation", defaultImpl = IdentifiersManager.class)
	public static Identified ident4;
	@DeclareParents(value = "io.quantumdb.core.migration.Migrator", defaultImpl = IdentifiersManager.class)
	public static Identified ident5;
	@DeclareParents(value = "io.quantumdb.core.schema.definitions.Column", defaultImpl = IdentifiersManager.class)
	public static Identified ident6;
//	@DeclareParents(value = "io.quantumdb.core.backends.DatabaseMigrator", defaultImpl = IdentifiersManager.class)
//	public static Identified ident7;
//	
	public static EventHelper<BGMEventListener> bgmm = new EventHelper<>(BGMEventListener.class);
	// Bea: lo creo para las operaciones anidadas, para que me cree ficheros
	// independientes
	public static EventHelper<BGMEventListener> nestedBgmm = new EventHelper<>(BGMEventListener.class);
	static {

//		bgmm.addListener(new ListenerCSV());
//		bgmm.addListener(new ListenerCSV2());
		/// Bea:cambio porque solo me generaba un archivo
		// bgmm.addListener(new ListenerJSON());

	}
	private String DMOsql = null;
	private Config conf = null;
	private String author = null;

	private boolean insertSelect = false;
	// Para enlazar un copy con un insert
	private boolean previousCopy = false;
	private boolean schemaOperation = false;
	//////////////// Para insert select
	// El rowId del último registro de la tabla Insert
	int lastRowId = 0;
	// El número de registros que se insertarán del sourceTable
	int numberSelectCount = 0;

	//String bea=null;
	private static final String EXECUTION_ID = IdentifiersManager.randomUUID();

//	//For obtaining a VersionIdGenerator which allows us to generate columns' values identifiers
//	@Pointcut("execution(* io.quantumdb.core.versioning.ChangeLog.new(..))")
//	public void versionIdGenerator() {
//	}

	@Pointcut("execution (io.quantumdb.core.backends.Config.new(..))")
	public void connection() {
	}

	@Pointcut("execution(* io.quantumdb.core.versioning.ChangeSet.getAuthor())")
	public void authorToPROV() {
	}

	@Pointcut("execution(* io.quantumdb.core.migration.operations.CreateTableMigrator.migrate(..))")
	public void createTableToPROV() {
	}

	@Pointcut("execution(* io.quantumdb.core.migration.operations.CopyTableMigrator.migrate(..))")
	public void copyTableToPROV() {
		//bea = "CreateTable";
	}

	@Pointcut("execution (io.quantumdb.core.schema.operations.DataOperation.new(..))")
	public void catchSQLInDMO() {
	}

	// io.quantumdb.core.backends.DatabaseMigrator.applyDataChanges
	// @Pointcut("execution(* io.quantumdb.core.migration.Migrator.migrate(..))")
	// @Pointcut("execution(*
	// *.applySchemaChanges(io.quantumdb.core.versioning.State,
	// io.quantumdb.core.migration.Migrator.Stage))")
	// @Pointcut("execution(* io.quantumdb.core.migration.Migrator.migrate(..))")
	@Pointcut("call(* io.quantumdb.core.backends.DatabaseMigrator+.applyDataChanges(io.quantumdb.core.versioning.State,io.quantumdb.core.migration.Migrator.Stage))")
	// @Pointcut("this(io.quantumdb.core.backends.DatabaseMigrator)")
	// @Pointcut("execution(* io.quantumdb.core.migration.Migrator.migrate(..))")
	public void insertDMOToPROV() {
	}

	// catchSQLInDMO
	@Before("catchSQLInDMO()")
	public void logCatchSQLQuery(JoinPoint joinPoint) {

		Object[] signatureArgs = joinPoint.getArgs();
		String DMOsqlAux = ((String) signatureArgs[0]).trim();
		DMOsql = DMOsqlAux;
		//////////// System.out.println("3333333333333333333333333333333333333333333333");
		int indexFirstLeftBracket = DMOsqlAux.indexOf("(");
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String jdbcUrl = conf.getUrl();
		String jdbcUser = conf.getUser();
		String jdbcPass = conf.getPassword();
		// Es un insert select
		if (indexFirstLeftBracket == -1) {
			insertSelect = true;
			String[] palabras = DMOsqlAux.split("\\s+");
			// ArrayList<Object> resultado = new ArrayList<Object>();
			// Bea resultado.put("insertSelect", insertSelect);
			String targetTableName = palabras[2];
			String select = palabras[3] + " count(*) from ";
			// Lo hago así porque igual después del from hay where, etc. que limita el
			// número de registros
			//////////// System.out.println("tamaño " + palabras.length);
			for (int i = 6; i < palabras.length; i++) {

				select = select + " " + palabras[i];

			}
			try {
				con = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
				ps = con.prepareStatement("select row_number() over() from " + targetTableName
						+ " order by row_number() over() desc limit 1");
				rs = ps.executeQuery();
				if (rs.next()) {
					//////////// System.out.println("número de filas previo al insert " +
					//////////// lastRowId);
					lastRowId = rs.getInt(1);
				}
				rs.close();
				ps.close();

			} catch (SQLException e) {
				// TODO: handle exception
				// e.printStackTrace();
				System.out.println("no existiría la tabla");

			}
			try {
				ps = con.prepareStatement(select);
				//////////// System.out.println("select es " + select);
				rs = ps.executeQuery();
				if (rs.next()) {

					numberSelectCount = rs.getInt(1);
					//////////// System.out.println("número de filas que se insertarán es " +
					//////////// numberSelectCount);
				}
				rs.close();
				ps.close();

			} catch (SQLException e) {
				// TODO: handle exception
				e.printStackTrace();
			} finally {
				try {
					if (con != null)
						con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
//		} else {
//			
//			String aux = DMOsqlAux.substring(DMOsqlAux.indexOf("INTO")).trim();
//			String aux2 = aux.substring(4).trim();
//			int indexBlank = aux2.indexOf(" ");
//			String targetTableName = aux2.substring(0, indexBlank);
//		}
	}

	// Elimino este before porque no me creaba un fichero por operación. He tenido
	// que crear un listener en cada operación (cada listener equivale a un fichero)
	// y andar añadiéndolo al BGM y eliminándolo en función del provenance que vaya
	// generando

//	/**********************************************************/
//	/************************ Start operator ********************/
//	/**********************************************************/
//	// || this(targetIdentified)
//	@Before("(createTableToPROV() || copyTableToPROV() || insertDMOToPROV()) ") // && (target(targetIdentified) )")
//	public void beforeExecution(JoinPoint joinPoint) {// , Identified targetIdentified) {//, Identified
//														// dataTargetIdentified) {
//		// Bea: lo pongo aquí con la idea de que me cree un fichero por operación
//		bgmm.addListener(new ListenerJSON());
//		UUID id = new UUID(joinPoint.getTarget());
////		System.out.println("entra before" + joinPoint.getTarget());
////		System.out.println(joinPoint.getTarget());
////		System.out.println(targetIdentified);
//		// Todo esto lo hice porque si se ha ejecutado un DML, el target no me dejaba
//		// indicar con el declare que implementara Identified (realmente el target hacía
//		// referencia a una clase del paquete de postgresql) y en esos casos he tenido
//		// que coger el this e indicar que el this implementa (declare) la interfaz
//		// Identified
//		Identified targetIdentified = null;
//		if (joinPoint.getTarget() instanceof Identified) {
//			targetIdentified = (Identified) joinPoint.getTarget();
//		} else
//			targetIdentified = (Identified) joinPoint.getThis();
//
////		if (schemaTargetIdentified == null)
////			targetIdentified = dataTargetIdentified;
////		else
////			targetIdentified = schemaTargetIdentified;
//		(targetIdentified).setUUID(id);
////		targetIdentified.setUUID(id);
////		System.out.println("target "+ targetIdentified.getUUID());
////		// final String ID_MSG = auxOperationType(joinPoint) + "-" +
////		// IdentifiersManager.randomUUID();
////		System.out.println("entra before");
//		final String ID_MSG = id.getUUID();
////		System.out.println("ID BEFORE" + ID_MSG);
////		System.out.println("targetIdentified" + targetIdentified);
////		System.out.println("EXECUTION_ID" + EXECUTION_ID);
//		System.out.println("class name" + targetIdentified.getClass().getSimpleName());
//		BGMEvent initMethod = new BGMEvent(targetIdentified, EXECUTION_ID, targetIdentified.getClass().getSimpleName(),
//				ID_MSG);
//		try {
//			bgmm.fireEvent("operationStart", initMethod);
//		} catch (InvocationTargetException e) {
//			e.printStackTrace();
//		}
//
//	}

//	@After("versionIdGenerator()") 
//	public void obtainGenerator(JoinPoint joinPoint) {
//		Changelog cl= (Changelog)joinPoint.getTarget();
//		cl.getIdGenerator().generateID();
//		
//
//	}

	@After("connection()")
	public void con(JoinPoint joinPoint) {
		conf = (Config) joinPoint.getTarget();
	}

	@AfterReturning(pointcut = "authorToPROV()", returning = "aut")
	public void logAuthor(JoinPoint joinPoint, String aut) {
		if (!aut.equalsIgnoreCase("QuantumDB")) {
//			UUID id = IdentifiersManager.getIdentifier(joinPoint.getTarget());
//			final String ID_MSG = id.getUUID();
//			provAuthor(joinPoint, ID_MSG, author);
			// Cambié lo anterior porque aparecía un executionIdMehod diferente el general
			// de la operación y aparecían un monton de bindings de author
			author = aut;
		}
	}

	@AfterThrowing("createTableToPROV() || copyTableToPROV() || insertDMOToPROV()")
	public void logCreateTableError(JoinPoint joinPoint) throws Exception {
		UUID id = IdentifiersManager.getIdentifier(joinPoint.getTarget());
		final String ID_MSG = id.getUUID();
		String operatorType = auxOperation(joinPoint);
		if (!operatorType.equals("CreateTable") && !operatorType.equals("CopyTable"))
			operatorType = "Insert";
		// Bea: %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
		// %%%%%%%%%%%%%%%%%% MEJOR UNA VARIALBE GLOBAL PORQUE ESTA START DEBERÍA SER EL
		// COMIENZO
		String start = String.valueOf(new Timestamp(System.currentTimeMillis()));
		provErrorEndOperation(joinPoint, ID_MSG, operatorType, operatorType, start);
	}

	@After("createTableToPROV() || copyTableToPROV() || insertDMOToPROV()")
	// @After(" insertDMOToPROV()")
	public void logOperations(JoinPoint joinPoint) {
		String operationType = null;
		boolean enter = false;
		// Bea, insertSelect = false;
		UUID id = IdentifiersManager.getIdentifier(joinPoint.getTarget());
////////////System.out.println("entra after" + joinPoint.getTarget());
		// System.out.println("DMOSQL es "+ DMOsql);
		final String ID_MSG = id.getUUID();

//		System.out.println("ID after" + ID_MSG);
//		System.out.println("EXECUTION_ID" + EXECUTION_ID);
//		System.out.println(joinPoint.getSignature().getClass().getName());

		// public void migrate(Catalog catalog, RefLog refLog, Version version,
		// CreateTable operation) {
		// public void migrate(Catalog catalog, RefLog refLog, Version version,
		// CopyTable operation) {
		// ******************Como en Copy **************************
		RefLog refLog = null;
		Version version = null;
		Changelog changelog = null;
		String sourceTableName = null;
		String targetTableName = null;
		String previousName = null;
		Version sourceSchemaVersion = null, targetSchemaVersion = null;
		String sourceSchemaVersionID = null, targetSchemaVersionID = null;
		String catalogName = null;
		Catalog catalog = null;
		State state = null;
		Stage stage = null;

		UUID idColumnUUID = null;
		String idColumn = null;
		TableRef sourceTableRef = null, targetTableRef = null;
		Table sourceTable = null, targetTable = null;
		ImmutableList<ColumnDefinition> columnsDef = null;
		ImmutableList<Column> columns = null;
////////////System.out.println("Signature name : " + joinPoint.getSignature().getName());
		Object[] signatureArgs = joinPoint.getArgs();
		HashMap<Object, Object> valoresDMO = null;

		schemaOperation = false;
		for (Object signatureArg : signatureArgs) {
			// System.out.println("Arg: " + signatureArg);
			// Cambia respecto al CopyTable
			if (signatureArg instanceof io.quantumdb.core.schema.operations.SchemaOperation) {
				schemaOperation = true;
				if (signatureArg instanceof io.quantumdb.core.schema.operations.CreateTable) {
					//////////// System.out.println("entra a create");
					operationType = "CreateTable";
					targetTableName = ((CreateTable) signatureArg).getTableName();
					columnsDef = ((CreateTable) signatureArg).getColumns();
				}
				if (signatureArg instanceof io.quantumdb.core.schema.operations.CopyTable) {
					//////////// System.out.println("entra a copy");
					operationType = "CopyTable";
					sourceTableName = ((CopyTable) signatureArg).getSourceTableName();
					targetTableName = ((CopyTable) signatureArg).getTargetTableName();
				}
			} else if (signatureArg instanceof io.quantumdb.core.versioning.State) {
				operationType = "dmo";
				state = (State) signatureArg;
			}
			if (signatureArg instanceof io.quantumdb.core.migration.Migrator.Stage) {
				stage = (Stage) signatureArg;
				sourceSchemaVersionID = stage.getParent().getId();
				targetSchemaVersionID = stage.getLast().getId();

			}
			if (signatureArg instanceof io.quantumdb.core.versioning.RefLog) {
				refLog = (RefLog) signatureArg;
			}
			if (signatureArg instanceof io.quantumdb.core.versioning.Version) {
				version = (Version) signatureArg;
			}
			if (signatureArg instanceof io.quantumdb.core.schema.definitions.Catalog) {
				catalog = (Catalog) signatureArg;
			}
		}
////////////System.out.println("operationType es " + operationType);
		/**********************************************************/
		/*************** Obtaining data from operators ***********/
		/**********************************************************/
		// Obtaining data from SMOs
		if (!operationType.equalsIgnoreCase("dmo")) {
			catalogName = catalog.getName();
			sourceSchemaVersion = version.getParent();
			targetSchemaVersion = version;
			sourceSchemaVersionID = version.getParent().getId();
			targetSchemaVersionID = version.getId();
			//////////// System.out.println("Versions aspecto (la anterior)" +
			//////////// refLog.getVersions() + "--" + sourceSchemaVersion);
			//////////// System.out.println("Versions getParent (la anterior)" +
			//////////// version.getParent() + "--" + sourceSchemaVersion);
			//////////// System.out.println("Versions version (la nueva)" + version + "--" +
			//////////// targetSchemaVersion);

			targetTableRef = refLog.getTableRef(version, targetTableName);
			//////////// System.out.println("Refid de " + targetTableName + " es " +
			//////////// targetTableRef.getRefId());
			targetTable = catalog.getTable(targetTableRef.getRefId());
			//////////// System.out.println(targetTableRef.getName());
			//////////// System.out.println(targetTableRef.getRefId());
			// In Copy SMO there is a source table
			if (operationType.equalsIgnoreCase("CopyTable")) {
				sourceTableRef = refLog.getTableRef(sourceSchemaVersion, sourceTableName);
				sourceTable = catalog.getTable(sourceTableRef.getRefId());
				columns = targetTable.getColumns();
			}
			//////////// System.out.println("En copy DMOSQL es " + DMOsql);

			valoresDMO = auxProcessDMO(DMOsql, catalog, refLog, sourceSchemaVersion, targetSchemaVersion);
		}
		// Obtaining data from DMOs
		else {
			refLog = state.getRefLog();
			catalog = state.getCatalog();
			changelog = state.getChangelog();
			//////////// System.out.println("ultima version añadida" +
			//////////// state.getChangelog().getLastAdded());
			//////////// System.out.println(sourceSchemaVersionID);
			//////////// System.out.println(targetSchemaVersionID);
			sourceSchemaVersion = changelog.getVersion(sourceSchemaVersionID);
			targetSchemaVersion = changelog.getVersion(targetSchemaVersionID);
			DMOsql = ((DataOperation) targetSchemaVersion.getOperation()).getQuery();
			//////////// System.out.println("DMOSQL es " + DMOsql);
			//////////// System.out.println("operation en migrate en aspecto: "+
			//////////// ((DataOperation) targetSchemaVersion.getOperation()).getQuery());
			valoresDMO = auxProcessDMO(DMOsql, catalog, refLog, sourceSchemaVersion, targetSchemaVersion);
			// BeaSystem.out.println(valoresDMO.get("insertSelect"));
			// Miro a ver si es insertSelect
			if (insertSelect) {
				//////////// System.out.println("Entra a insertSelect");
				targetTableRef = (TableRef) valoresDMO.get("targetTable");
				sourceTableRef = (TableRef) valoresDMO.get("sourceTable");
				targetTableName = targetTableRef.getName();
				sourceTableName = sourceTableRef.getName();
				//////////// System.out.println("targetTableName " + targetTableName);
				//////////// System.out.println("sourceTableName " + sourceTableName);
				// sourceTableRef = refLog.getTableRef(sourceSchemaVersion, sourceTableName);

				//////////// System.out.println("sourceSchemaVersion " + sourceSchemaVersion);
				//////////// System.out.println("targetSchemaVersion " + targetSchemaVersion);

				// Bea: pongo target schema
//				for (TableRef t : refLog.getTableRefs()) {
//
//					System.out.println(" tabla " + t.getName() + " " + t.getRefId() + " " + t.getVersions());
//
//				}
				targetTable = catalog.getTable(targetTableRef.getRefId());
				// Cojo las target porque me da igual source que target porque solo usaré los
				// nombres y tipos
				columns = targetTable.getColumns();
			}
			// Else --> insert simples
			else {
				targetTableRef = (TableRef) valoresDMO.get("targetTable");
				targetTableName = targetTableRef.getName();
				//////////// System.out.println("beaaaaaaaaaaaatargetTableName " +
				//////////// targetTableName);
				// targetTableRef = refLog.getTableRef(sourceSchemaVersion, targetTableName);
				// ImmutableList<Column> allColumnsTable = targetTable.getColumns();
				// En este caso obtengo igualmente las columnas
				Set<Object> objectsSet = valoresDMO.keySet();
				//////////// System.out.println("Tamaño" + objectsSet.size());
				// BeavaloresDMO.remove("insertSelect");
				// valoresDMO.remove("targetTable");
				// valoresDMO.remove("type");
				//////////// System.out.println("Tamaño" + objectsSet.size());
				targetTable = catalog.getTable(targetTableRef.getRefId());
				columns = targetTable.getColumns();
				Set<Column> columnsSet = new HashSet<Column>();
//				ArrayList<Column> columnsArrayList = new ArrayList<Column>();
//				columns= Collections.unmodifiableList(columnsArrayList) ;
//				for (Object o : objectsSet) {
//					columnsSet.add((Column) o);
//				}
				//////////////////////////////////////
				//////////////////////////////////////
				// No sé muy bien si lo utilizaré luego
//				columns = ImmutableList.copyOf(columnsSet);
//				int i = 2;
//				String type = (String) valores.get("type");
//				if (type.equalsIgnoreCase("allColumns")) {
//					for(i=2; i<valores.size()-2; i++) {
//						columns[i-2]= valores.get(i)
//					}
//				}
			}

		}

		/**********************************************************/
		/********************* Generating entities ****************/
		/**********************************************************/
		if (operationType.equalsIgnoreCase("CreateTable")) {

			createToProv(joinPoint, ID_MSG, catalog, refLog, version, sourceSchemaVersionID, targetSchemaVersionID,
					targetTableRef, true);

		} else if (operationType.equalsIgnoreCase("CopyTable")) {

			copyToProv(joinPoint, ID_MSG, catalog, refLog, version, sourceSchemaVersionID, targetSchemaVersionID,
					sourceTableRef, targetTableRef, valoresDMO);

		} else if (operationType.equalsIgnoreCase("dmo"))

		{
			// System.out.println("Invoca a DMOToProv");
			DMOToProv(joinPoint, ID_MSG, catalog, targetTableRef, valoresDMO);
		}

//		String operation = auxOperation(joinPoint);
//
//		provStartOperation(joinPoint, ID_MSG, operation);
//		provAuthor(joinPoint, ID_MSG, author);
//		if (operationType.equalsIgnoreCase("Create")) {
//			provInputValue(joinPoint, ID_MSG, "inputTableName", "tableName", targetTableName);
//			provSourceSchema(joinPoint, ID_MSG, sourceSchemaVersionID, catalogName);
//			provTargetSchema(joinPoint, ID_MSG, targetSchemaVersionID, catalogName);
//			provComplexTable(joinPoint, ID_MSG, "targetTable", targetTableRef.getRefId(), targetTableName);
//			// Target table's columns
//			for (ColumnDefinition col : columnsDef) {
//				idColumnUUID = new UUID(col);
//				idColumn = idColumnUUID.getUUID();
//				provSimpleColumn(joinPoint, ID_MSG, col.getName(), "inputColumnDef", idColumn);
//				provComplexColumn(joinPoint, ID_MSG, col.getName(), col.getType().toString(), "targetColumn", idColumn);
//			}
//			// Previous tables
//			ImmutableSet<Table> tables = catalog.getTables();
//			for (Table t : tables) {
//				previousName = t.getName();
//				System.out.println("+" + previousName);
//				if (!previousName.equalsIgnoreCase(targetTableRef.getRefId()))
//					provSimpleTable(joinPoint, ID_MSG, "previousTable",
//							refLog.getTableRef(version.getParent(), previousName).getRefId(), previousName);
//			}
//		} else if (operationType.equalsIgnoreCase("Copy")) {
//			previousCopy = true;
//			////////////////////////////////////////////////
//			//// Generating Copy PROV elements
//			////////////////////////////////////////////////
//			provInputValue(joinPoint, ID_MSG, "targetTableName", "tableName", targetTableName);
//			provSourceSchema(joinPoint, ID_MSG, sourceSchemaVersionID, catalogName);
//			provTargetSchema(joinPoint, ID_MSG, targetSchemaVersionID, catalogName);
//			provSimpleTable(joinPoint, ID_MSG, "sourceTable", sourceTableRef.getRefId(), sourceTableName);
//			provComplexTable(joinPoint, ID_MSG, "targetTable", targetTableRef.getRefId(), targetTableName);
//			// Target table's columns
//			for (Column col : columns) {
//				idColumnUUID = new UUID(col);
//				idColumn = idColumnUUID.getUUID();
//				provSimpleColumn(joinPoint, ID_MSG, col.getName(), "sourceColumn", idColumn);
//				provComplexColumn(joinPoint, ID_MSG, col.getName(), col.getType().toString(), "targetColumn", idColumn);
//			}
//			// Previous tables
//			ImmutableSet<Table> tables = catalog.getTables();
//			for (Table t : tables) {
//				previousName = t.getName();
//				System.out.println("+" + previousName);
//				if (!previousName.equalsIgnoreCase(targetTableRef.getRefId()))
//					provSimpleTable(joinPoint, ID_MSG, "previousTable",
//							refLog.getTableRef(version.getParent(), previousName).getRefId(), previousName);
//			}
//			////////////////////////////////////////////////
//			//// Generating Create PROV elements in COPY
//			////////////////////////////////////////////////
//			// OJO PORQUE TENGO QUE SIMULARLO
//			final String nested_create_ID_MSG = ID_MSG + "Create";
//			provStartOperation(joinPoint, nested_create_ID_MSG, "CreateTable");
//			provInputValue(joinPoint, ID_MSG, "inputTableName", "tableName", targetTableName);
//			provSourceSchema(joinPoint, ID_MSG, sourceSchemaVersionID, catalogName);
//			provTargetSchema(joinPoint, ID_MSG, targetSchemaVersionID, catalogName);
//			provComplexTable(joinPoint, ID_MSG, "targetTable", targetTableRef.getRefId(), targetTableName);
//			// Target table's columns
//			for (Column col : columns) {
//				idColumnUUID = new UUID(col);
//				idColumn = idColumnUUID.getUUID();
//				provSimpleColumn(joinPoint, ID_MSG, col.getName(), "inputColumnDef", idColumn);
//				provComplexColumn(joinPoint, ID_MSG, col.getName(), col.getType().toString(), "targetColumn", idColumn);
//			}
//
//			provSuccessEndOperation(joinPoint, nested_create_ID_MSG);
//
//			provSuccessEndOperation(joinPoint, ID_MSG);
//
//		} else if (operationType.equalsIgnoreCase("DMO")) {
//			////////////////////////////////////////////////
//			//// Generating DMO PROV elements
//			////////////////////////////////////////////////
//
//			if (previousCopy) {
//
//			} else {
//				provSimpleTable(joinPoint, ID_MSG, "table", targetTableRef.getRefId(), targetTableName);
//
////			for (Column col : columns) {
////				idColumnUUID = new UUID(col);
////				idColumn = idColumnUUID.getUUID();
////				//provSimpleColumn(joinPoint, ID_MSG, col.getName(), col.getType().toString(), "column", idColumn);
//
//				// Target table columns's values and their columns
//				System.out.println("invoca a provComplexColumnValues con" + valoresDMO);
//				provComplexColumnValues(joinPoint, ID_MSG, "column", "newColumnValue", valoresDMO, catalog);
////			}
//			}
//		}

		/**********************************************************/
		/************************ End operator ********************/
		/**********************************************************/
//		provSuccessEndOperation(joinPoint, ID_MSG);
//		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID,
//				joinPoint.getTarget().getClass().getSimpleName(), ID_MSG);
		String operatorType = auxOperation(joinPoint);
		if (!operatorType.equals("CreateTable") && !operatorType.equals("CopyTable"))
			operatorType = "Insert";
		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID, operatorType, ID_MSG);

		try {
			bgmm.fireEvent("operationEnd", initMethod);
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void createToProv(JoinPoint joinPoint, String ID_MSG, Catalog catalog, RefLog refLog, Version targetVersion,
			String sourceSchemaVersionID, String targetSchemaVersionID, TableRef targetTableRef, boolean independent) {

		String operation = null;
//		if (independent)
//			operation = auxOperation(joinPoint);
//		else
		operation = "CreateTable";
		String operatorType = operation;
		String catalogName = catalog.getName();
		String idColumn = null, previousName = null;
		String targetTableName = targetTableRef.getName();
		Table targetTable = catalog.getTable(targetTableRef.getRefId());
		ImmutableList<Column> columns = targetTable.getColumns();
		UUID idColumnUUID = null;

		/**********************************************************/
		/*********************** Create listener ******************/
		/**********************************************************/
		// Lo cambio para poner el de Mongo
		// ListenerJSON lis = initiateListener(joinPoint, operatorType);
		// System.out.println("Crea listener para Create");
		// System.out.println("Invoca a initiateListener con "+operatorType + " y "+
		// ID_MSG);
		SetBindingsListener lis = initiateListener(joinPoint, ID_MSG, operatorType);
		///// ListenerMongoDB_expanded lis = initiateListener(joinPoint, ID_MSG,
		///// operatorType);
		// System.out.println("lis es "+ lis);
		// bgmm.addListener(lis);

		///// Si no es independiente, entonces no se va a crear un archivo para su
		///// provenance. Por ello incluyo las instrucciones adecuadas para ello
		/**********************************************************/
		/************************ start operator ********************/
		/**********************************************************/
//**************Lo comenté porque el start opration ya lo tiene en initiateListener
//		if (!independent) {
//			////// Ojo porque pongo el mismo identificador que el copy
//			Identified targetIdentified = (Identified) joinPoint.getTarget();
//
//			// nestedBgmm.addListener(new ListenerJSON());
////			BGMEvent initMethod = new BGMEvent(targetIdentified, EXECUTION_ID,
////					targetIdentified.getClass().getSimpleName(), ID_MSG);
		// System.out.println("en el create antes invocaba con "+ ID_MSG);
//			BGMEvent initMethod = new BGMEvent(targetIdentified, EXECUTION_ID,
//					operatorType, ID_MSG);
//			try {
//				System.out.println("operation start con "+operatorType + " y "+ ID_MSG);
//				bgmm.fireEvent("operationStart", initMethod);
//			} catch (InvocationTargetException e) {
//				e.printStackTrace();
//			}
//
//		}

		/// Bea: lo comento para meterlo en el endoperation y así está todo junto
		// provStartOperation(joinPoint, ID_MSG, operation, operation);
		String start = String.valueOf(new Timestamp(System.currentTimeMillis()));
		if (independent)
			provAuthor(joinPoint, ID_MSG, author, operation);

		provInputValue(joinPoint, ID_MSG, "inputTableName", "inTableName", targetTableName, operatorType);
		provSourceSchema(joinPoint, ID_MSG, sourceSchemaVersionID, catalogName, operatorType);
		provTargetSchema(joinPoint, ID_MSG, targetSchemaVersionID, catalogName, operatorType);
		provComplexTable(joinPoint, ID_MSG, "targetTable", targetTableRef.getRefId(), targetTableName, operatorType);
		// Target table's columns
		for (Column col : columns) {
			idColumnUUID = new UUID(col);
			// idColumn = idColumnUUID.getUUID();
			idColumn = "Column" + col.hashCode() + "_" + targetTableName;
			System.out.println("Invoca 3 a provSimpleColumn con " + col.getName() + " y " + idColumn);
			// A inputColumnef le añado al identificador un "Def" al final para que no se
			// identifique de igual forma que la columna que generará
			/////////////////////////////// 7
			provSimpleColumn(joinPoint, ID_MSG, col.getName(), "inputColumnDef", col.getType().toString(),
					idColumn + "Def", operatorType);
			//////////////////////////////////
			provComplexColumn(joinPoint, ID_MSG, col.getName(), col.getType().toString(), "targetColumn", idColumn,
					"inputColumnDef", idColumn + "Def", operatorType);
		}
		// Previous tables
		ImmutableSet<Table> tables = catalog.getTables();
		for (Table t : tables) {
			previousName = t.getName();
			//////////// System.out.println("+" + previousName);
			if (!previousName.equalsIgnoreCase(targetTableRef.getRefId()))
				provSimpleTable(joinPoint, ID_MSG, "previousTable",
						refLog.getTableRef(targetVersion.getParent(), previousName).getRefId(), previousName,
						operatorType);
		}

		provSuccessEndOperation(joinPoint, ID_MSG, operation, operatorType, start);

		/**********************************************************/
		/************************ End operator ********************/
		/**********************************************************/
		if (!independent) {
//			BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID,
//					joinPoint.getTarget().getClass().getSimpleName(), ID_MSG);
			BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID, operatorType, ID_MSG);
			try {
				bgmm.fireEvent("operationEnd", initMethod);
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		bgmm.removeListener(lis);
	}

	public void copyToProv(JoinPoint joinPoint, String ID_MSG, Catalog catalog, RefLog refLog, Version targetVersion,
			String sourceSchemaVersionID, String targetSchemaVersionID, TableRef sourceTableRef,
			TableRef targetTableRef, HashMap<Object, Object> valoresDMO) {

		previousCopy = true;

		String operation = auxOperation(joinPoint);
		String operatorType = "CopyTable";
		String catalogName = catalog.getName();
		String idColumnSource = null, idColumnTarget = null, previousName = null;
		String sourceTableName = sourceTableRef.getName();
		String targetTableName = targetTableRef.getName();
		Table targetTable = catalog.getTable(targetTableRef.getRefId());
		ImmutableList<Column> columns = targetTable.getColumns();
		UUID idColumnUUID = null;

		/**********************************************************/
		/*********************** Create listener ******************/
		/**********************************************************/
		// ListenerJSON lis = initiateListener(joinPoint,"CreateType");
		// System.out.println("Crea listener para copy");
		// System.out.println("Invoca a initiateListener con "+operatorType + " y "+
		// ID_MSG);
		//// ListenerMongoDB_bindings2 lis = initiateListener(joinPoint, ID_MSG,
		// operatorType);

		// System.out.println("lis es "+ lis);
		// bgmm.addListener(lis);

		///// Si no es independiente, entonces no se va a crear un archivo para su
		///// provenance. Por ello incluyo las instrucciones adecuadas para ello
		/**********************************************************/
		/************************ start operator ********************/
		/**********************************************************/
		/// Bea: lo comento para meterlo en el endoperation y así está todo junto
		// provStartOperation(joinPoint, ID_MSG, operation, operation);
		String start = String.valueOf(new Timestamp(System.currentTimeMillis()));

		////////////////////////////////////////////////
		//// Generating Create PROV elements in COPY
		////////////////////////////////////////////////
		// OJO PORQUE TENGO QUE SIMULARLO
		// bgmm.removeListener(lis);

		final String nested_create_ID_MSG = ID_MSG + "Create";

		createToProv(joinPoint, nested_create_ID_MSG, catalog, refLog, targetVersion, sourceSchemaVersionID,
				targetSchemaVersionID, targetTableRef, false);
		////////////////////////////////////////////////
		//// End Create PROV elements in COPY
		////////////////////////////////////////////////
		// bgmm.addListener(lis);

		/**********************************************************/
		/************************ End operator ********************/
		/**********************************************************/
		SetBindingsListener lis = initiateListener(joinPoint, ID_MSG, operatorType);

		provAuthor(joinPoint, ID_MSG, author, operatorType);
		provInputValue(joinPoint, ID_MSG, "targetTableName", "inTableName", targetTableName, operatorType);
		provSourceSchema(joinPoint, ID_MSG, sourceSchemaVersionID, catalogName, operatorType);
		provTargetSchema(joinPoint, ID_MSG, targetSchemaVersionID, catalogName, operatorType);
		provSimpleTable(joinPoint, ID_MSG, "sourceTable", sourceTableRef.getRefId(), sourceTableName, operatorType);
		provComplexTable(joinPoint, ID_MSG, "targetTable", targetTableRef.getRefId(), targetTableName, operatorType);
		// Target table's columns
		for (Column col : columns) {
			idColumnUUID = new UUID(col);
			// idColumn = idColumnUUID.getUUID();
			idColumnTarget = "Column" + col.hashCode() + "_" + targetTableName;
			idColumnSource = "Column" + col.hashCode() + "_" + sourceTableName;
			System.out.println("Invoca a provSimpleColumn con " + col.getName() + " y " + idColumnTarget + "y con "
					+ idColumnSource);
			provSimpleColumn(joinPoint, ID_MSG, col.getName(), "sourColumn", null, idColumnSource, operatorType);
			provComplexColumn(joinPoint, ID_MSG, col.getName(), col.getType().toString(), "targetColumn",
					idColumnTarget, "sourColumn", idColumnSource, operatorType);// Beita
		}
		// Target columns' values

		provComplexColumnValues(joinPoint, ID_MSG, "targetColumn", "targetColumnValue", valoresDMO, catalog,
				operatorType);

		// Previous tables
		ImmutableSet<Table> tables = catalog.getTables();
		for (Table t : tables) {
			previousName = t.getName();
			//////////// System.out.println("+" + previousName);
			if (!previousName.equalsIgnoreCase(targetTableRef.getRefId()))
				provSimpleTable(joinPoint, ID_MSG, "previousTable",
						refLog.getTableRef(targetVersion.getParent(), previousName).getRefId(), previousName,
						operatorType);
		}
		provSuccessEndOperation(joinPoint, ID_MSG, operation, operatorType, start);
//		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID,
//				joinPoint.getTarget().getClass().getSimpleName(), ID_MSG);
		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID, operatorType, ID_MSG);
		try {
			bgmm.fireEvent("operationEnd", initMethod);
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		bgmm.removeListener(lis);

	}

	public void DMOToProv(JoinPoint joinPoint, String ID_MSG, Catalog catalog, TableRef targetTableRef,
			HashMap<Object, Object> valoresDMO) {
		// System.out.println("Entra a DMOToProv con valoresDMO " + valoresDMO);
		/**********************************************************/
		/*********************** Create listener ******************/
		/**********************************************************/
		// ListenerJSON lis = initiateListener(joinPoint, "Insert");
		// System.out.println("Crea listener para insert");
		// System.out.println("Invoca a initiateListener con insert y " + ID_MSG);
		////ListenerMongoDB_bindings2 lis = initiateListener(joinPoint, ID_MSG, "Insert");
		String start = String.valueOf(new Timestamp(System.currentTimeMillis()));
		SetBindingsListener lis = initiateListener(joinPoint, ID_MSG, "Insert");
		// System.out.println("lis es " + lis);
		// bgmm.addListener(lis);
		////////////////////////////////////////////////
		//// Generating DMO PROV elements
		////////////////////////////////////////////////
		String targetTableName = targetTableRef.getName();
		String operation = auxOperation(joinPoint);
		String operatorType = "Insert";
		/// Bea: lo comento para meterlo en el endoperation y así está todo junto
		// provStartOperation(joinPoint, ID_MSG, operation, operation);

		// Bea: de gestionar más tipos de DML, se tendría que especificar la operación
//******************************************************************
		provAuthor(joinPoint, ID_MSG, author, "Insert");
		provSimpleTable(joinPoint, ID_MSG, "table", targetTableRef.getRefId(), targetTableName, operatorType);

		provComplexColumnValues(joinPoint, ID_MSG, "column", "newColumnValue", valoresDMO, catalog, operatorType);

		provSuccessEndOperation(joinPoint, ID_MSG, operation, operatorType, start);
//******************************************************************
		/**********************************************************/
		/************************ End operator ********************/
		/**********************************************************/

//		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID,
//				joinPoint.getTarget().getClass().getSimpleName(), ID_MSG);
		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID, operatorType, ID_MSG);
		try {
			bgmm.fireEvent("operationEnd", initMethod);
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		bgmm.removeListener(lis);
	}

//ListenerMongoDB_bindings2 ListenerJSON
	// ListenerMongoDB_expanded
	public SetBindingsListener initiateListener(JoinPoint joinPoint, String ID_MSG, String operatorType) {

		SetBindingsListener lis = new SetBindingsListener();
		// ListenerJSON lis = new ListenerJSON();

		bgmm.addListener(lis);
		UUID id = new UUID(joinPoint.getTarget());
		Identified targetIdentified = null;
		if (joinPoint.getTarget() instanceof Identified) {
			targetIdentified = (Identified) joinPoint.getTarget();
		} else // Tuve que distinguir entre las DMO y el resto porque en las DMO no existía el
				// target
			targetIdentified = (Identified) joinPoint.getThis();
//		System.out.println("antes el ID_MSG era "+id.getUUID());
//		 id = IdentifiersManager.getIdentifier(targetIdentified);
//		System.out.println("Ahora el ID_MSG es "+id.getUUID());
//		final String ID_MSG = id.getUUID();
//		BGMEvent initMethod = new BGMEvent(targetIdentified, EXECUTION_ID, targetIdentified.getClass().getSimpleName(),
//				ID_MSG);

		BGMEvent initMethod = new BGMEvent(targetIdentified, EXECUTION_ID, operatorType, ID_MSG);
		try {
			// System.out.println("operation start con " + operatorType + " y " + ID_MSG);
			bgmm.fireEvent("operationStart", initMethod);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return lis;
	}

	public HashMap<Object, Object> auxProcessDMO(String sql, Catalog catalog, RefLog refLog,
			Version sourceSchemaVersion, Version targetSchemaVersion) {
		// System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Procesa consulta");
//		String aux= DMOsql.substring(DMOsql.indexOf("INTO")).trim();
//		String aux2= aux.substring(4).trim();
//		String targetTable= aux2.substring(0, aux2.indexOf(" "));
//		System.out.println("aux"+aux);
//		System.out.println("aux2"+aux2);
//		System.out.println("targetTable"+targetTable);

		////////////////// Funcionaba en insert-select para coger solamente las tablas
		////////////////// origen y destino, pero decidí considerar cuando es un insert
		////////////////// normal
		//////////////////
		// Beaboolean insertSelect = false;
		int i = 1;
//		String[] palabras = DMOsql.split("\\s+");
//		ArrayList<Object> resultado = new ArrayList<Object>();
//		if (palabras[3].equalsIgnoreCase("select"))
//			insertSelect = true;
//		resultado.set(0, insertSelect);
//		resultado.set(1, palabras[2]);
//		resultado.set(2, palabras[6].substring(0, palabras[6].length() - 1));
//		return resultado;
		//////////////////
		//////////////////
		//////////////////
		//////////////////
////////////System.out.println("ENTRAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
		int indexFirstLeftBracket = DMOsql.indexOf("(");
		HashMap<Object, Object> resultado = new HashMap<Object, Object>();
		// Es un insert select
		if (indexFirstLeftBracket == -1) {
			insertSelect = true;
			String[] palabras = DMOsql.split("\\s+");
			// ArrayList<Object> resultado = new ArrayList<Object>();
			// Bea resultado.put("insertSelect", insertSelect);
			String targetTableName = palabras[2];
			String sourceTableName = palabras[6].substring(0, palabras[6].length() - 1);

			// En el caso de que justo antes se haya creado la tabla, no estará con el
			// nombre "normal" en el target version
			// Cuando ejecutaba un insert normal con tablas con nombres "normales" el
			// targetTableName estaba en el targetSchemaVersion
			// if(targetSchemaVersion.)
			//////////// System.out.println(catalog.containsTable(targetTableName));
//			for (Table t : catalog.getTables()) {
//
//				System.out.println(t.getName());
//
//			}

//			System.out.println("sourceSchemaVersion");
//			for(TableRef t: refLog.getTableRefs(sourceSchemaVersion)) {
//				System.out.println("table ref es "+t.getName());
//				
//				
//			}
//			System.out.println("targetSchemaVersion");
//			for(TableRef t: refLog.getTableRefs(targetSchemaVersion)) {
//				System.out.println("table ref es "+t.getName());
//				
//				
//			}
			TableRef targetTableRef = null;
			try {
				targetTableRef = refLog.getTableRef(targetSchemaVersion, targetTableName);

			} catch (IllegalArgumentException e) {
				targetTableRef = refLog.getTableRef(sourceSchemaVersion, targetTableName);
			}
			Table targetTable = catalog.getTable(targetTableRef.getRefId());
			ImmutableList<Column> targetColumns = targetTable.getColumns();
			TableRef sourceTableRef = null;
			try {
				sourceTableRef = refLog.getTableRef(targetSchemaVersion, sourceTableName);

			} catch (IllegalArgumentException e) {
				sourceTableRef = refLog.getTableRef(sourceSchemaVersion, sourceTableName);
			}

			Table sourceTable = catalog.getTable(sourceTableRef.getRefId());
			System.out.println(" targetTableName es " + targetTableName);
			System.out.println("$$$$$$$$$$$$ mete como source " + sourceTableRef.getName() + " y como target "
					+ targetTableRef.getName());
			resultado.put("targetTable", targetTableRef);
			resultado.put("sourceTable", sourceTableRef);
			ImmutableList<Column> sourceColumns = sourceTable.getColumns();

			for (i = 0; i < targetColumns.size(); i++) {
				// System.out.println("Target column " + targetColumns.get(i).getName() +
				// "source column "+ sourceColumns.get(i).getName());
				resultado.put(targetColumns.get(i), sourceColumns.get(i));
			}

		} else {
			// String[] palabras = DMOsql.split("(");
			String aux = DMOsql.substring(DMOsql.indexOf("INTO")).trim();
			String aux2 = aux.substring(4).trim();
			int indexBlank = aux2.indexOf(" ");
			String targetTableName = aux2.substring(0, indexBlank);
			int indexFirstRightBracket = DMOsql.indexOf(")");
			int indexSecondLeftBracket = DMOsql.lastIndexOf("(");
			int indexSecondRightBracket = DMOsql.lastIndexOf(")");
			//////////// System.out.println("indexFirstLeftBracket" +
			//////////// indexFirstLeftBracket);
			//////////// System.out.println("indexFirstRigthBracket" +
			//////////// indexFirstRightBracket);
			//////////// System.out.println("indexSecondLeftBracket" +
			//////////// indexSecondLeftBracket);
			//////////// System.out.println("indexSecondLeftBracket" +
			//////////// indexSecondLeftBracket);

			// Solo hay un paréntesis --> insert in all columns
			if (indexFirstLeftBracket == indexSecondLeftBracket) {
				//////////// System.out.println("Insert all columns");
				String bracket = DMOsql.substring(indexFirstLeftBracket + 1, indexFirstRightBracket);
				String[] arrayValues = bracket.split(",");
				// Bea: en un insert con values cambio de sourceSchema a target porque source no
				// lo tenía

				TableRef targetTableRef = refLog.getTableRef(targetSchemaVersion, targetTableName);
				Table targetTable = catalog.getTable(targetTableRef.getRefId());
				// Bearesultado.put("insertSelect", insertSelect);
				resultado.put("targetTable", targetTableRef);
				resultado.put("type", "allColumns");
				ImmutableList<Column> columns = targetTable.getColumns();
				//////////// System.out.println("bracket " + bracket);
				i = 0;

				while (i < arrayValues.length) {
					//////////// System.out.println("a la columna " + columns.get(i).getName() + "
					//////////// le mete el value " + arrayValues[i]);
					resultado.put(columns.get(i), arrayValues[i]);
					i++;
				}
			}
			// Varios paréntesis --> insert of concrete columns
			else {
				//////////// System.out.println("Insert concrete columns");
				String firstBracket = DMOsql.substring(indexFirstLeftBracket + 1, indexFirstRightBracket);
				//////////// System.out.println("firstBracket " + firstBracket);
				String secondBracket = DMOsql.substring(indexSecondLeftBracket + 1, indexSecondRightBracket);
				//////////// System.out.println("secondBracket " + secondBracket);
//				String bracket1 = aux.substring(indexFirstLeftBracket+1, indexFirstRightBracket);
//				String bracket2 = aux.substring(indexSecondLeftBracket+1, indexSecondRightBracket);
//				System.out.println("bracket 1" + bracket1);
//				System.out.println("bracket 2" + bracket2);
				String[] arrayColumns = firstBracket.split(",");
				String[] arrayValues = secondBracket.split(",");

				//////////// System.out.println("targetTableName" + targetTableName + "--");
				TableRef targetTableRef = refLog.getTableRef(targetSchemaVersion, targetTableName);
				Table targetTable = catalog.getTable(targetTableRef.getRefId());
				// Bearesultado.put("insertSelect", insertSelect);
				resultado.put("targetTable", targetTable);
				resultado.put("type", "concreteColumns");
				// ImmutableList<Column> columns = targetTable.getColumns();
				//////////// System.out.println("bracket 1 " + firstBracket);
				//////////// System.out.println("bracket 2 " + secondBracket);
				i = 0;
				while (i < arrayValues.length) {
					//////////// System.out.println("Column " + arrayColumns[i]);
					//////////// System.out.println("value " + arrayValues[i]);
					resultado.put(targetTable.getColumn(arrayColumns[i]), arrayValues[i]);
					i++;
				}
			}

		}

		return resultado;
	}

	public String auxOperation(JoinPoint joinPoint) {
		String methodName = joinPoint.getSignature().getName(), fullClassName, className = null;
		int aux;
		String aux2 = null;
		// if (DMOsql == null) {
		if (schemaOperation) {
			fullClassName = joinPoint.getTarget().getClass().getName();
			className = fullClassName.substring(fullClassName.lastIndexOf(".") + 1);
			//////////// System.out.println(className);
			aux = className.lastIndexOf("Migrator");
			aux2 = className.substring(0, aux);
			//////////// System.out.println("NO dmo OPERATOR ES" + aux2);
			return aux2;
		} else {
			//////////// System.out.println("DEVUELVE COMO OPERATION " + DMOsql);
			return DMOsql;
		}
	}

	// Al ver que en PostgreSQL los ROWID cambian si se eliminan registros (se
	// reordenan) ha decidido no usar el rowId que considera el SGBD, como rowID.
	// Por ello, he decidido utilizar como rowID el mismo valor que la clave
	// primaria (si hay varias, concatenadas con "_").
	// Para conformar el identificador de cada valor de una columna, consideraré la
	// siguiente estructura: idColumn _ columnName _ PKValue (si
	// la PK está compuesta por varias columnas, se concatenan sus valores
	// consecuentemente). Hago notar que
	// la entidad de un valor de una columna está relacionada con la entidad de la
	// columna correpondiente, pero para que se vea claramente mirando la entidad
	// valor, he considerado también en primer lugar el id de la columna.
	// Este método devuelve el id del valor de la columna.
	private String auxColumnValueID(ArrayList<String> keyValues, String idColumn, String tableName, String columnName) {

		return idColumn + "_" + columnName + "_" + auxRowID(keyValues);

	}

//	private String auxRowID(Table table) {
//		String id = "";
//		boolean enter = false;
//		for (Column c : table.getPrimaryKeyColumns()) {
//			System.out.println("columna clave "+c.getName());
//			if (enter)
//				id = id+"_";
//			enter = true;
//			id = id+c.getName();
//		}
//		return id;
//	}

	private String auxRowID(ArrayList<String> keyValues) {
		String id = "";
		boolean enter = false;
		for (String s : keyValues) {
			if (enter)
				id = id + "_";
			enter = true;
			id = id + s;
		}
		return id;
	}

	public String auxOperationType() {

		// if (DMOsql == null)
		if (schemaOperation)
			return "smo";
		else
			return "dmo";
	}

	private void provStartOperation(JoinPoint joinPoint, String ID_MSG, String operation, String operatorType,
			String start) {
		Identified target = null;
		// inicio operacion
		// if (DMOsql == null) {
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
////////////System.out.println("Operation" + operation);
		// String operatorName = operation.substring(0, operation.indexOf(" "));
		String aux = operation, operator = aux;
		if (aux.indexOf(" ") != -1)
			operator = aux.substring(0, aux.indexOf(" "));
		System.out.println("operator es " + operator);
		provenance(EXECUTION_ID, target, ID_MSG, auxOperationType(), ID_MSG, operatorType);
		provenanceValueType(target, "operatorName", EXECUTION_ID, ID_MSG, completeID, "type", operator, operatorType);
		provenanceValueType(target, "startTransTime", EXECUTION_ID, ID_MSG, completeID, "startTransTime", start,
				operatorType);
		provenanceValueType(target, "operationInstruction", EXECUTION_ID, ID_MSG, completeID, "instruction", operation,
				operatorType);

	}

	private void provSuccessEndOperation(JoinPoint joinPoint, String ID_MSG, String operation, String operatorType,
			String start) {
		// cuando finaliza la operacion
		Identified target = null;
		// if (DMOsql == null) {
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
////////////System.out.println("target provEndOperation " + target);
////////////System.out.println("EXECUTION_ID " + EXECUTION_ID);
////////////System.out.println("ID_MSG " + ID_MSG);
////////////System.out.println(" completeID " + completeID);
		/// Bea: lo pongo aquí para que esté todo junto
		provStartOperation(joinPoint, ID_MSG, operation, operatorType, start);
		provenanceValueType(target, "executed", EXECUTION_ID, ID_MSG, completeID, "executed", "true", operatorType);
		provenanceValueType(target, "endTransTime", EXECUTION_ID, ID_MSG, completeID, "endTransTime",
				String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);

	}

	private void provErrorEndOperation(JoinPoint joinPoint, String ID_MSG, String operation, String operatorType,
			String start) {
		// cuando finaliza la operacion
		Identified target = null;
		// if (DMOsql == null) {
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
////////////System.out.println("target provEndOperation " + target);
////////////System.out.println("EXECUTION_ID " + EXECUTION_ID);
////////////System.out.println("ID_MSG " + ID_MSG);
////////////System.out.println(" completeID " + completeID);
		/// Bea: lo pongo aquí para que esté todo junto
		provStartOperation(joinPoint, ID_MSG, operation, operatorType, start);

		provenanceValueType(target, "executed", EXECUTION_ID, ID_MSG, completeID, "executed", "false", operatorType);
		provenanceValueType(target, "endTransTime", EXECUTION_ID, ID_MSG, completeID, "endTransTime",
				String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);

	}

	private void provSourceSchema(JoinPoint joinPoint, String ID_MSG, String schemaID, String catalogName,
			String operatorType) {
		Identified target = (Identified) joinPoint.getTarget();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		provenance(EXECUTION_ID, target, ID_MSG, "sourceSchema", schemaID, operatorType);
		provenanceValueType(target, "ssEndTransTime", EXECUTION_ID, ID_MSG, completeID, "endTransTime",
				String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
		provenanceValueType(target, "sSchemaName", EXECUTION_ID, ID_MSG, completeID, "schemaName", catalogName,
				operatorType);
	}

	private void provTargetSchema(JoinPoint joinPoint, String ID_MSG, String schemaID, String catalogName,
			String operatorType) {
		Identified target = (Identified) joinPoint.getTarget();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		provenance(EXECUTION_ID, target, ID_MSG, "targetSchema", schemaID, operatorType);
		provenanceValueType(target, "tsStartTransTime", EXECUTION_ID, ID_MSG, completeID, "startTransTime",
				String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
		provenanceValueType(target, "tShemaName", EXECUTION_ID, ID_MSG, completeID, "schemaName", catalogName,
				operatorType);
	}

	private void provSimpleTable(JoinPoint joinPoint, String ID_MSG, String varName, String tableID, String tableName,
			String operatorType) {
		Identified target = null;
		String prefix = "";
		// if (DMOsql == null) {
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		provenance(EXECUTION_ID, target, ID_MSG, varName, tableID, operatorType);
		if (varName.equalsIgnoreCase("sourceTable"))
			prefix = "s";

		else if (varName.equalsIgnoreCase("previousTable"))
			prefix = "p";

		provenanceValueType(target, prefix + "TableName", EXECUTION_ID, ID_MSG, completeID, "tableName", tableName,
				operatorType);
	}

	private void provComplexTable(JoinPoint joinPoint, String ID_MSG, String varName, String tableID, String tableName,
			String operatorType) {
		Identified target = (Identified) joinPoint.getTarget();
		String prefix1 = "", prefix2 = "";
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		provenance(EXECUTION_ID, target, ID_MSG, varName, tableID, operatorType);
		if (varName.equalsIgnoreCase("targetTable")) {
			prefix1 = "tt";
			prefix2 = "t";
		}

		provenanceValueType(target, prefix1 + "StartTransTime", EXECUTION_ID, ID_MSG, completeID, "startTransTime",
				String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
		provenanceValueType(target, prefix2 + "TableName", EXECUTION_ID, ID_MSG, completeID, "tableName", tableName,
				operatorType);
	}

	private void provInputValue(JoinPoint joinPoint, String ID_MSG, String varEntityName, String varAttributeName,
			String value, String operatorType) {
		Identified target = null;
		// if (DMOsql == null) {
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		// Ojo con el id del nombre de la tabla
		// Me ponía como targetTableName el operatorType
		provenance(EXECUTION_ID, target, ID_MSG, varEntityName, completeID, operatorType);
		provenanceValueType(target, varAttributeName, EXECUTION_ID, ID_MSG, completeID, "value", value, operatorType);
	}

//Linked: en create el targetColumn is linked con inputColumnDef
//		  en Copy targetColumn is linked con sourceColumn
// paso el id con el que tienen que linkarse a través de sourceInputColumnId
	private void provComplexColumn(JoinPoint joinPoint, String ID_MSG, String columnName, String columnType,
			String nameVar, String ID_COL, String linkedVar, String linkedValue, String operatorType) {
		String prefix1 = "", prefix2 = "";
		Identified target = null;
		// if (DMOsql == null) {
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		provenance(EXECUTION_ID, target, ID_MSG, nameVar, ID_COL, operatorType);
		if (nameVar.equalsIgnoreCase("targetColumn")) {
			prefix1 = "tc";
			prefix2 = "t";

		}
		provenanceValueType(target, prefix1 + "StartTransTime", EXECUTION_ID, ID_MSG, completeID, "startTransTime",
				String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
		provenanceValueType(target, prefix2 + "ColumnName", EXECUTION_ID, ID_MSG, completeID, "columnName", columnName,
				operatorType);
		provenanceValueType(target, prefix2 + "ColumnType", EXECUTION_ID, ID_MSG, completeID, "typeName", columnType,
				operatorType);
		/////// En copy y create
		if (nameVar.equals("targetColumn")) {
			//
			provenanceValueType(target, linkedVar, EXECUTION_ID, ID_MSG, completeID, "linked", linkedValue,
					operatorType);

		}
	}

	private void provSimpleColumn(JoinPoint joinPoint, String ID_MSG, String columnName, String varName,
			String columnType, String ID_COL, String operatorType) {
		Identified target = null;
		String prefix = "";
		// if (DMOsql == null) {
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		System.out.println("En provSimpleColumn la columna con id " + ID_COL + " es " + columnName + " con " + varName);
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		System.out.println("completeID " + completeID);
		provenance(EXECUTION_ID, target, ID_MSG, varName, ID_COL, operatorType);
		if (varName.equalsIgnoreCase("sourColumn")) {
			prefix = "s";
		}
		else if (varName.equalsIgnoreCase("inputColumnDef")) {
			prefix = "in";
		}

		provenanceValueType(target, prefix + "ColumnName", EXECUTION_ID, ID_MSG, completeID, "columnName", columnName,
				operatorType);
		if (columnType != null)
			provenanceValueType(target, prefix + "ColumnType", EXECUTION_ID, ID_MSG, completeID, "typeName",
					columnType, operatorType);
		// provenanceValueType(target, "columnType", EXECUTION_ID, ID_MSG, completeID,
		// "typeName", columnType);
	}

	private void provSimpleColumnValue(JoinPoint joinPoint, String ID_MSG, String columnValueVar, String columnName,
			String columnValue, String ID_COL_Value, String rowId, String operatorType) {
		Identified target = null;
		String prefix = "";
		// if (DMOsql == null) {
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		provenance(EXECUTION_ID, target, ID_MSG, columnValueVar, ID_COL_Value, operatorType);
		if (columnValueVar.equalsIgnoreCase("sourceColumnValue")) {
			prefix = "s";
		}
		else if (columnValueVar.equalsIgnoreCase("value")) {
			prefix = "v";
		}
		provenanceValueType(target, prefix + "ColumnValue", EXECUTION_ID, ID_MSG, completeID, "columnValue",
				columnValue, operatorType);
		provenanceValueType(target, prefix + "RowId", EXECUTION_ID, ID_MSG, completeID, "rowID", rowId, operatorType);
	}

	private void provSimplestColumnValue(JoinPoint joinPoint, String ID_MSG, String columnValueVar, String value,
			String ID_COL_Value, String operatorType) {
		Identified target = null;
		// if (DMOsql == null) {
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		provenance(EXECUTION_ID, target, ID_MSG, columnValueVar, ID_COL_Value, operatorType);
		provenanceValueType(target, "columnValue", EXECUTION_ID, ID_MSG, completeID, "columnValue", value,
				operatorType);
	}

	private void provComplexColumnValues(JoinPoint joinPoint, String ID_MSG, String columnVar, String columnValueVar,
			HashMap<Object, Object> valoresDMOmethod, Catalog catalog, String operatorType) {
		// System.out.println("%%%%%%%%Entra en provComplexColumnValues6666");
		Identified target = null;
		// if (DMOsql == null) {
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		/////////////////////////
		Connection con = null;
		PreparedStatement ps = null;
		ResultSetMetaData rsM = null;
		ResultSet rs = null;
		String prefix = "", prefix2= "";
		String jdbcUrl = conf.getUrl();
		String jdbcUser = conf.getUser();
		String jdbcPass = conf.getPassword();
		Set<Object> columnasTarget = null;
		List<Object> columnasTargetList = new ArrayList<Object>();
		//////////////////////////////////////
		String targetColumnValueId, sourceColumnValueId = null;
		String columnValue = null, rowId = null;
		Table sourceTable = null;
		Table targetTable = null;
		TableRef sourceTableRef = null;
		TableRef targetTableRef = null;
		UUID idTargetColumnUUID, idSourceColumnUUID = null;
		String idTargetColumn, idSourceColumn = null;
		String sourceColumnName = null, targetColumnName = null;
		Column targetColumn = null, sourceColumn = null;
		Set<Object> columnasInsert = null;
		List<Object> columnasInsertList = new ArrayList<Object>();
		// ArrayList<Object> rowIdValues = null;
		ArrayList<String> keyValues = new ArrayList<String>();
		ArrayList<Column> keyColumns = new ArrayList<Column>();
		// ArrayList<String> keyColumnsNames= new ArrayList<String>();
		int i = 1, numColumns = 0;
		int numKeyColumns = 0;
		String value = null;
		// try {
		// System.out.println("valoresDMO.get(\"insertSelect\")" + insertSelect);
		String keyColumnsSelect = "";
		boolean entra = false;
		// Insert select operation
		// Por ahora solo imagino que es un select de TODO
		if (insertSelect) {
			try {
				con = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
//System.out.println("Bea"+valoresDMOmethod);
				sourceTableRef = (TableRef) valoresDMOmethod.get("sourceTable");
				targetTableRef = (TableRef) valoresDMOmethod.get("targetTable");
				//////// Devuelve el nombre "normal"
				// System.out.println("55555555 targetTableRef "+targetTableRef.getName());
				targetTable = catalog.getTable(targetTableRef.getRefId());
				// Devuelve el nombre CODIFICADOOOOOOOOOO
				// System.out.println("55555555 targetTable "+targetTable.getName());
				// Solo para el caso del copy porque no se había creado la tabla y así consulto
				// sobre la del source
				sourceTable = catalog.getTable(sourceTableRef.getRefId());
				keyColumns = (ArrayList<Column>) targetTable.getPrimaryKeyColumns();
				for (Column c : keyColumns) {
					numKeyColumns++;
					if (!entra) {
						keyColumnsSelect = c.getName();
						entra = true;

					} else
						keyColumnsSelect = keyColumnsSelect + "," + c.getName();

				}
				//////////// System.out.println("consulta de la tabla " +
				//////////// targetTable.getName());
				valoresDMOmethod.remove("sourceTable");
				valoresDMOmethod.remove("targetTable");
				// En el caso de que venga de un COPY todavía no se había creado la tabla (me
				// decía que no existía)
				// por lo que he decidido consultar en la de origen
				if (schemaOperation)
					ps = con.prepareStatement("select " + keyColumnsSelect + ", * from " + sourceTable.getName()
							+ " order by row_number() over() desc limit " + numberSelectCount);
				else
					// rECUERDA QUE CONSULTA CON EL NOMBRE CODIFICADO
					ps = con.prepareStatement("select " + keyColumnsSelect + ", * from " + targetTable.getName()
							+ " order by row_number() over() desc limit " + numberSelectCount);
				rs = ps.executeQuery();
				rsM = ps.getMetaData();
				numColumns = rsM.getColumnCount();
				//////////// System.out.println("numColumns " + numColumns);

				columnasTarget = (Set<Object>) valoresDMOmethod.keySet();
				// System.out.println("tamaño de columnasTarget "+ columnasTarget.size());
				// columnasTargetList = new ArrayList<Object>();
				columnasTargetList.addAll(columnasTarget);
				// System.out.println("tamaño de columnasTargetList "+
				// columnasTargetList.size());
//			List<Object> columnasSourceList = new ArrayList<Object>();
//			columnasSourceList.addAll(columnasSource);

				targetColumn = null;
				sourceColumn = null;
				int contadorFilas = 0;
				while (rs.next()) {
					contadorFilas = 1;
					// if (i == 1) {
					// rowId = rs.getObject(i).toString();
					keyValues.clear();
					for (int j = 1; j <= numKeyColumns; j++) {
						keyValues.add(rs.getObject(j).toString());

					}
					i = numKeyColumns + 1;
					while (i <= numColumns) {
						//////////// System.out.println(i);
						// targetColumnName = rsM.getColumnName(i);

						//////////// System.out.println("columnName" + targetColumnName);
						columnValue = rs.getObject(i).toString();

						targetColumn = (Column) columnasTargetList.get(i - 1 - numKeyColumns);
						// System.out.println("targetColumn es "+ targetColumn.getName());
						// sourceColumn = (Column)columnasSourceList.get(i-1);
						/////////////////////////////// Associated Columns in target
						idTargetColumnUUID = new UUID(targetColumn);
						// idTargetColumn = idTargetColumnUUID.getUUID();
						//// TARGETTABLE .GETNAME DEVOLVÍA EL CODIFICADOOOOOOO (EN COPY TABLE, NO CREADO
						idTargetColumn = "Column" + targetColumn.hashCode() + "_" + targetTableRef.getName();
						targetColumnName = targetColumn.getName();
						//////////////////////////////////
						////////////////////////////// Source Values in source
						sourceColumn = (Column) valoresDMOmethod.get(targetColumn);

						sourceColumnName = sourceColumn.getName();
						// System.out.println("valoresDMOmethod tiene asociada la columna
						// "+sourceColumnName+" con la columna"+ targetColumnName);
						idSourceColumnUUID = new UUID(sourceColumn);
						// idSourceColumn = idSourceColumnUUID.getUUID();
						idSourceColumn = "Column" + sourceColumn.hashCode() + "_" + sourceTable.getName();
						columnValue = columnValue.replace("'", "");
						/////////////////////////////////////////////////////
						// Las columns solamente pueden estar una vezz en el provenance (y tendrán
						// tantos newColumnValue como registros insertados
						if (contadorFilas == 1) {
							System.out.println("Invoca -1 a provSimpleColumn con " + targetColumn.getName() + " y "
									+ idTargetColumn);
							//// Me lo creaba DOBLEEEEEEEE PORQUE YA LO CREO ANTES EN UN FOR QUE RECORRE
							//// OLUMNAS
//							if (columnValueVar.equalsIgnoreCase("targetColumnValue")) {
//								System.out.println("entra");
////								provSimpleColumn(joinPoint, ID_MSG, targetColumnName, columnVar, idTargetColumn,
////										operatorType);
//								System.out.println("Invoca -1 a provSimpleColumn con " + sourceColumnName + " y "
//										+ idSourceColumn);
//								// "sourceColumn"
//								System.out.println("columnVar es " + columnVar);
//								provSimpleColumn(joinPoint, ID_MSG, sourceColumnName, "sourColumn", idSourceColumn,
//										operatorType);
//							} else
							if (columnValueVar.equalsIgnoreCase("newColumnValue")) {
								System.out.println("entra Bea con " + targetColumnName);
								provSimpleColumn(joinPoint, ID_MSG, targetColumnName, columnVar, null, idTargetColumn,
										operatorType);

							}
						}
						////////////////////////////// Source Values in source

						System.out.println("bea " + sourceTable.getName() + " y " + targetTable.getName());
						///////////////// He considerado que la clave del source es el mismo que el
						///////////////// target por lo que el rowId del valor del source es el mismo
						///////////////// que el del target. Si no, se podría coger en el select del
						///////////////// target los valores de la clave del source (al igualque se han
						///////////////// cogido del target, para conformar los valores del "keyValues"
						///////////////// con los que se invocaría a estos métodos auxiliares
						sourceColumnValueId = auxColumnValueID(keyValues, idSourceColumn, sourceTable.getName(),
								sourceColumnName);
						System.out.println("sourceColumnValueId es " + sourceColumnValueId + " de " + sourceColumnName
								+ " en la tabla " + targetTable.getName() + "columnValue");

						if (columnValueVar.equalsIgnoreCase("newColumnValue")) {

							provSimpleColumnValue(joinPoint, ID_MSG, "value", sourceColumnName, columnValue,
									sourceColumnValueId, auxRowID(keyValues), operatorType);
						} else if (columnValueVar.equalsIgnoreCase("targetColumnValue")) {
							provSimpleColumnValue(joinPoint, ID_MSG, "sourceColumnValue", sourceColumnName, columnValue,
									sourceColumnValueId, auxRowID(keyValues), operatorType);
						}

						System.out.println("La columna con id " + idTargetColumn + " es " + targetColumnName);
						/////////////////////////////// Columns' values in target
						targetColumnValueId = auxColumnValueID(keyValues, idTargetColumn, targetTable.getName(),
								targetColumnName);
						System.out.println("targetColumnValueId es " + targetColumnValueId + " de " + targetColumnName
								+ " en la tabla " + targetTable.getName());
						provenance(EXECUTION_ID, target, ID_MSG, columnValueVar, targetColumnValueId, operatorType);
						if (columnValueVar.equalsIgnoreCase("targetColumnValue")) {
							prefix = "t";
							prefix2= "tcv";
						}
						else if (columnValueVar.equalsIgnoreCase("newColumnValue")) {
							prefix = "nv";
							prefix2=prefix;
						}
						provenanceValueType(target, prefix2+ "StartTransTime", EXECUTION_ID, ID_MSG, completeID,
								"startTransTime", String.valueOf(new Timestamp(System.currentTimeMillis())),
								operatorType);
						provenanceValueType(target, prefix + "ColumnValue", EXECUTION_ID, ID_MSG, completeID,
								"columnValue", columnValue, operatorType);
						provenanceValueType(target, prefix + "RowId", EXECUTION_ID, ID_MSG, completeID, "rowID",
								auxRowID(keyValues), operatorType);

						/////////////// Linked//////////////////////
						/////// En insert
						if (columnValueVar.equals("newColumnValue")) {

							provenanceValueType(target, "value", EXECUTION_ID, ID_MSG, completeID, "linked",
									sourceColumnValueId, operatorType);
							provenanceValueType(target, "column", EXECUTION_ID, ID_MSG, completeID, "linked",
									idTargetColumn, operatorType);

//							provenanceValueType(target, "value", EXECUTION_ID, ID_MSG, completeID, "linked",
//									"linked", operatorType);
//							provenanceValueType(target, "column", EXECUTION_ID, ID_MSG, completeID, "linked",
//									"linked", operatorType);

						}
						////// En Copy
						else if (columnValueVar.equals("targetColumnValue")) {

							provenanceValueType(target, "targetColumn", EXECUTION_ID, ID_MSG, completeID, "linked",
									idTargetColumn, operatorType);
							provenanceValueType(target, "sourceColumnValue", EXECUTION_ID, ID_MSG, completeID, "linked",
									sourceColumnValueId, operatorType);

						}

						i++;
					}
					// i = 1;

				}
				ps.close();

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// Usual insert
		else {
			targetTableRef = (TableRef) valoresDMOmethod.get("targetTable");
			targetTable = catalog.getTable(targetTableRef.getRefId());
			keyColumns = (ArrayList<Column>) targetTable.getPrimaryKeyColumns();
			// System.out.println("rrrrrrrrrrrrrrrrrrrrrrrrrrrr" + targetTable);
			//////////// System.out.println("rrrrrrrrrrrrrrrrrrrrrrrrrrrr" +
			//////////// targetTable.getName());
			valoresDMOmethod.remove("type");
			valoresDMOmethod.remove("targetTable");
			columnasInsert = (Set<Object>) valoresDMOmethod.keySet();
			columnasInsertList.addAll(columnasInsert);
			for (Column c : keyColumns) {
				keyValues.add(valoresDMOmethod.get(c).toString().replace("'", ""));
			}
			rowId = auxRowID(keyValues);
			i = 1;
			numColumns = columnasInsert.size();
			while (i <= numColumns) {
				//////////// System.out.println(i);

				targetColumn = (Column) columnasInsertList.get(i - 1);
				targetColumnName = targetColumn.getName();
				//////////// System.out.println("columnName" + targetColumnName);
				// columnValue = rs.getObject(i).toString();

				columnValue = (String) valoresDMOmethod.get(targetColumn);
				//////////// System.out.println("columnValue" + columnValue);
				/////////////////////////////// Associated Columns in target
				idTargetColumnUUID = new UUID(targetColumn);
				idTargetColumn = String.valueOf(targetColumn.hashCode());
				System.out.println("Asocia666" + targetColumnName + " y " + columnVar + " y " + idTargetColumn);
				provSimpleColumn(joinPoint, ID_MSG, targetColumnName, columnVar, null, idTargetColumn, operatorType);

				/////////////////////////////// Columns' values in target
				//////////// System.out.println("El que da error :" + columnValueVar);
				provenance(EXECUTION_ID, target, ID_MSG, columnValueVar,
						auxColumnValueID(keyValues, idTargetColumn, targetTable.getName(), targetColumnName),
						operatorType);
				provenanceValueType(target, "startTransTime", EXECUTION_ID, ID_MSG, completeID, "startTransTime",
						String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
				provenanceValueType(target, "columnValue", EXECUTION_ID, ID_MSG, completeID, "columnValue", columnValue,
						operatorType);
				provenanceValueType(target, "rowId", EXECUTION_ID, ID_MSG, completeID, "rowID", rowId, operatorType);
				////////////////////////////// Source Values in source
				value = (String) valoresDMOmethod.get(targetColumn);
				// De ser cadena, tendrá las comillas simples (aunque podría haber colocado
				// perfectamente el columnValue
				value = value.replace("'", "");
				provSimplestColumnValue(joinPoint, ID_MSG, "value", value, String.valueOf(value.hashCode()),
						operatorType);
				i++;

			}
		}
	}

	private void provAuthor(JoinPoint joinPoint, String ID_MSG, String author, String operatorType) {
		Identified target = null;
		// if (DMOsql == null) {
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();

		// String completeID =
		// IdentifiersManager.getIdentifier(target).getCompleteUUID();
		System.out.println("En provAuthor target es " + target + " y author es " + author);
		// Me daba un NullPointerException (LAS LIBRERÍAS DE PROV) porque el nombre del
		// author era "Michael de jong" imagino que por la ¿largura?
		provenance(EXECUTION_ID, target, ID_MSG, "user", author, operatorType);

	}

	private void provenance(String ID_EXECUTION, final Identified target, String ID_EXECUTION_METHOD,
			String variableName, String value, String operatorType) {
		try {
			// Bea: lo cambio para poner en limpio el tipo de operación
			// BGMEvent e = new BGMEvent(target, ID_EXECUTION,
			// target.getClass().getSimpleName(), ID_EXECUTION_METHOD,variableName, value);
			System.out.println("AsociaBinding en aspecto " + variableName + " con " + value);
			BGMEvent e = new BGMEvent(target, ID_EXECUTION, operatorType, ID_EXECUTION_METHOD, variableName, value);
			bgmm.fireEvent("newBinding", e);
		} catch (InvocationTargetException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	private void provenanceValueType(final Identified target, String variableName, String ID_EXECUTION,
			String ID_EXECUTION_METHOD, String identifier, String type, String value, String operatorType) {
		try {

			// BGMEvent e = new BGMEvent(target, ID_EXECUTION, value, identifier, type);
//			String aux= "\u0027";
//			System.out.println("value en provenanceValueType es "+value);
//			String newValue= StringUtils.replace(value,aux,"'");
//			System.out.println("newValue en provenanceValueType es "+newValue);
			// Bea: lo cambio para poner en limpio el tipo de operación
			// BGMEvent e = new BGMEvent(target, ID_EXECUTION,
			// target.getClass().getSimpleName(), ID_EXECUTION_METHOD,variableName, value);
			System.out.println("Asocia333 " + variableName + " con " + value);
			BGMEvent e = new BGMEvent(target, ID_EXECUTION, operatorType, ID_EXECUTION_METHOD, variableName, value);
			bgmm.fireEvent("newValueBinding", e);
		} catch (InvocationTargetException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

}