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

	public static EventHelper<BGMEventListener> bgmm = new EventHelper<>(BGMEventListener.class);
	
	public static EventHelper<BGMEventListener> nestedBgmm = new EventHelper<>(BGMEventListener.class);
	static {


	}
	private String DMOsql = null;
	private Config conf = null;
	private String author = null;

	private boolean insertSelect = false;
	
	private boolean previousCopy = false;
	private boolean schemaOperation = false;
	
	int lastRowId = 0;
	
	int numberSelectCount = 0;

	
	private static final String EXECUTION_ID = IdentifiersManager.randomUUID();


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
	
	}

	@Pointcut("execution (io.quantumdb.core.schema.operations.DataOperation.new(..))")
	public void catchSQLInDMO() {
	}

	@Pointcut("call(* io.quantumdb.core.backends.DatabaseMigrator+.applyDataChanges(io.quantumdb.core.versioning.State,io.quantumdb.core.migration.Migrator.Stage))")
	public void insertDMOToPROV() {
	}

	
	@Before("catchSQLInDMO()")
	public void logCatchSQLQuery(JoinPoint joinPoint) {

		Object[] signatureArgs = joinPoint.getArgs();
		String DMOsqlAux = ((String) signatureArgs[0]).trim();
		DMOsql = DMOsqlAux;
		int indexFirstLeftBracket = DMOsqlAux.indexOf("(");
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String jdbcUrl = conf.getUrl();
		String jdbcUser = conf.getUser();
		String jdbcPass = conf.getPassword();
		
		if (indexFirstLeftBracket == -1) {
			insertSelect = true;
			String[] palabras = DMOsqlAux.split("\\s+");
			
			String targetTableName = palabras[2];
			String select = palabras[3] + " count(*) from ";
			
			for (int i = 6; i < palabras.length; i++) {

				select = select + " " + palabras[i];

			}
			try {
				con = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
				ps = con.prepareStatement("select row_number() over() from " + targetTableName
						+ " order by row_number() over() desc limit 1");
				rs = ps.executeQuery();
				if (rs.next()) {
					
					lastRowId = rs.getInt(1);
				}
				rs.close();
				ps.close();

			} catch (SQLException e) {
				// TODO: handle exception
				// e.printStackTrace();
				System.out.println("Table does not exist");

			}
			try {
				ps = con.prepareStatement(select);
				rs = ps.executeQuery();
				if (rs.next()) {

					numberSelectCount = rs.getInt(1);
					
				}
				rs.close();
				ps.close();

			} catch (SQLException e) {
				
				e.printStackTrace();
			} finally {
				try {
					if (con != null)
						con.close();
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			}
		}

	}

	@After("connection()")
	public void con(JoinPoint joinPoint) {
		conf = (Config) joinPoint.getTarget();
	}

	@AfterReturning(pointcut = "authorToPROV()", returning = "aut")
	public void logAuthor(JoinPoint joinPoint, String aut) {
		if (!aut.equalsIgnoreCase("QuantumDB")) {
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
		
		String start = String.valueOf(new Timestamp(System.currentTimeMillis()));
		provErrorEndOperation(joinPoint, ID_MSG, operatorType, operatorType, start);
	}

	@After("createTableToPROV() || copyTableToPROV() || insertDMOToPROV()")
	public void logOperations(JoinPoint joinPoint) {
		String operationType = null;
		boolean enter = false;
		UUID id = IdentifiersManager.getIdentifier(joinPoint.getTarget());

		final String ID_MSG = id.getUUID();

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

		Object[] signatureArgs = joinPoint.getArgs();
		HashMap<Object, Object> valoresDMO = null;

		schemaOperation = false;
		for (Object signatureArg : signatureArgs) {
		
			if (signatureArg instanceof io.quantumdb.core.schema.operations.SchemaOperation) {
				schemaOperation = true;
				if (signatureArg instanceof io.quantumdb.core.schema.operations.CreateTable) {
					
					operationType = "CreateTable";
					targetTableName = ((CreateTable) signatureArg).getTableName();
					columnsDef = ((CreateTable) signatureArg).getColumns();
				}
				if (signatureArg instanceof io.quantumdb.core.schema.operations.CopyTable) {
					
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
		

			targetTableRef = refLog.getTableRef(version, targetTableName);
			
			targetTable = catalog.getTable(targetTableRef.getRefId());
			
			if (operationType.equalsIgnoreCase("CopyTable")) {
				sourceTableRef = refLog.getTableRef(sourceSchemaVersion, sourceTableName);
				sourceTable = catalog.getTable(sourceTableRef.getRefId());
				columns = targetTable.getColumns();
			}
			

			valoresDMO = auxProcessDMO(DMOsql, catalog, refLog, sourceSchemaVersion, targetSchemaVersion);
		}
	
		else {
			refLog = state.getRefLog();
			catalog = state.getCatalog();
			changelog = state.getChangelog();
			
			sourceSchemaVersion = changelog.getVersion(sourceSchemaVersionID);
			targetSchemaVersion = changelog.getVersion(targetSchemaVersionID);
			DMOsql = ((DataOperation) targetSchemaVersion.getOperation()).getQuery();
			
			valoresDMO = auxProcessDMO(DMOsql, catalog, refLog, sourceSchemaVersion, targetSchemaVersion);
			
			if (insertSelect) {
				
				targetTableRef = (TableRef) valoresDMO.get("targetTable");
				sourceTableRef = (TableRef) valoresDMO.get("sourceTable");
				targetTableName = targetTableRef.getName();
				sourceTableName = sourceTableRef.getName();
				
				targetTable = catalog.getTable(targetTableRef.getRefId());
				
				columns = targetTable.getColumns();
			}
			// Else --> insert simples
			else {
				targetTableRef = (TableRef) valoresDMO.get("targetTable");
				targetTableName = targetTableRef.getName();
				
				Set<Object> objectsSet = valoresDMO.keySet();
				
				targetTable = catalog.getTable(targetTableRef.getRefId());
				columns = targetTable.getColumns();
				Set<Column> columnsSet = new HashSet<Column>();
//				
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
		
			DMOToProv(joinPoint, ID_MSG, catalog, targetTableRef, valoresDMO);
		}


		/**********************************************************/
		/************************ End operator ********************/
		/**********************************************************/

		String operatorType = auxOperation(joinPoint);
		if (!operatorType.equals("CreateTable") && !operatorType.equals("CopyTable"))
			operatorType = "Insert";
		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID, operatorType, ID_MSG);

		try {
			bgmm.fireEvent("operationEnd", initMethod);
		} catch (InvocationTargetException e) {
			
			e.printStackTrace();
		}
	}

	public void createToProv(JoinPoint joinPoint, String ID_MSG, Catalog catalog, RefLog refLog, Version targetVersion,
			String sourceSchemaVersionID, String targetSchemaVersionID, TableRef targetTableRef, boolean independent) {

		String operation = null;

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
		
		SetBindingsListener lis = initiateListener(joinPoint, ID_MSG, operatorType);
		
		/**********************************************************/
		/************************ start operator ********************/
		/**********************************************************/

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
			
			idColumn = "Column" + col.hashCode() + "_" + targetTableName;
			
			
			provSimpleColumn(joinPoint, ID_MSG, col.getName(), "inputColumnDef", col.getType().toString(),
					idColumn + "Def", operatorType);
			
			provComplexColumn(joinPoint, ID_MSG, col.getName(), col.getType().toString(), "targetColumn", idColumn,
					"inputColumnDef", idColumn + "Def", operatorType);
		}
		// Previous tables
		ImmutableSet<Table> tables = catalog.getTables();
		for (Table t : tables) {
			previousName = t.getName();
			
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

			BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID, operatorType, ID_MSG);
			try {
				bgmm.fireEvent("operationEnd", initMethod);
			} catch (InvocationTargetException e) {
				
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
		
		/**********************************************************/
		/************************ start operator ********************/
		/**********************************************************/
		
		String start = String.valueOf(new Timestamp(System.currentTimeMillis()));

	

		final String nested_create_ID_MSG = ID_MSG + "Create";

		createToProv(joinPoint, nested_create_ID_MSG, catalog, refLog, targetVersion, sourceSchemaVersionID,
				targetSchemaVersionID, targetTableRef, false);
		

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
			
			idColumnTarget = "Column" + col.hashCode() + "_" + targetTableName;
			idColumnSource = "Column" + col.hashCode() + "_" + sourceTableName;
			
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
			
			if (!previousName.equalsIgnoreCase(targetTableRef.getRefId()))
				provSimpleTable(joinPoint, ID_MSG, "previousTable",
						refLog.getTableRef(targetVersion.getParent(), previousName).getRefId(), previousName,
						operatorType);
		}
		provSuccessEndOperation(joinPoint, ID_MSG, operation, operatorType, start);

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
		
		String start = String.valueOf(new Timestamp(System.currentTimeMillis()));
		SetBindingsListener lis = initiateListener(joinPoint, ID_MSG, "Insert");
		
		////////////////////////////////////////////////
		//// Generating DMO PROV elements
		////////////////////////////////////////////////
		String targetTableName = targetTableRef.getName();
		String operation = auxOperation(joinPoint);
		String operatorType = "Insert";
		
		provAuthor(joinPoint, ID_MSG, author, "Insert");
		provSimpleTable(joinPoint, ID_MSG, "table", targetTableRef.getRefId(), targetTableName, operatorType);

		provComplexColumnValues(joinPoint, ID_MSG, "column", "newColumnValue", valoresDMO, catalog, operatorType);

		provSuccessEndOperation(joinPoint, ID_MSG, operation, operatorType, start);

		/**********************************************************/
		/************************ End operator ********************/
		/**********************************************************/


		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID, operatorType, ID_MSG);
		try {
			bgmm.fireEvent("operationEnd", initMethod);
		} catch (InvocationTargetException e) {
		
			e.printStackTrace();
		}

		bgmm.removeListener(lis);
	}


	public SetBindingsListener initiateListener(JoinPoint joinPoint, String ID_MSG, String operatorType) {

		SetBindingsListener lis = new SetBindingsListener();
		

		bgmm.addListener(lis);
		UUID id = new UUID(joinPoint.getTarget());
		Identified targetIdentified = null;
		if (joinPoint.getTarget() instanceof Identified) {
			targetIdentified = (Identified) joinPoint.getTarget();
		} else // Tuve que distinguir entre las DMO y el resto porque en las DMO no existía el
				// target
			targetIdentified = (Identified) joinPoint.getThis();


		BGMEvent initMethod = new BGMEvent(targetIdentified, EXECUTION_ID, operatorType, ID_MSG);
		try {
		
			bgmm.fireEvent("operationStart", initMethod);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return lis;
	}

	public HashMap<Object, Object> auxProcessDMO(String sql, Catalog catalog, RefLog refLog,
			Version sourceSchemaVersion, Version targetSchemaVersion) {
	
		int i = 1;

		HashMap<Object, Object> resultado = new HashMap<Object, Object>();

		if (DMOsql != null) {

			int indexFirstLeftBracket = DMOsql.indexOf("(");

			// Is insert select
			if (indexFirstLeftBracket == -1) {
				insertSelect = true;
				String[] palabras = DMOsql.split("\\s+");
				
				String targetTableName = palabras[2];
				String sourceTableName = palabras[6].substring(0, palabras[6].length() - 1);

	
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
			
				resultado.put("targetTable", targetTableRef);
				resultado.put("sourceTable", sourceTableRef);
				ImmutableList<Column> sourceColumns = sourceTable.getColumns();

				for (i = 0; i < targetColumns.size(); i++) {
					
					resultado.put(targetColumns.get(i), sourceColumns.get(i));
				}

			} else {
			
				String aux = DMOsql.substring(DMOsql.indexOf("INTO")).trim();
				String aux2 = aux.substring(4).trim();
				int indexBlank = aux2.indexOf(" ");
				String targetTableName = aux2.substring(0, indexBlank);
				int indexFirstRightBracket = DMOsql.indexOf(")");
				int indexSecondLeftBracket = DMOsql.lastIndexOf("(");
				int indexSecondRightBracket = DMOsql.lastIndexOf(")");
				
				// Solo hay un paréntesis --> insert in all columns
				if (indexFirstLeftBracket == indexSecondLeftBracket) {
					
					String bracket = DMOsql.substring(indexFirstLeftBracket + 1, indexFirstRightBracket);
					String[] arrayValues = bracket.split(",");
				
					TableRef targetTableRef = refLog.getTableRef(targetSchemaVersion, targetTableName);
					Table targetTable = catalog.getTable(targetTableRef.getRefId());
				
					resultado.put("targetTable", targetTableRef);
					resultado.put("type", "allColumns");
					ImmutableList<Column> columns = targetTable.getColumns();
			
					i = 0;

					while (i < arrayValues.length) {
						
					
						resultado.put(columns.get(i), arrayValues[i]);
						i++;
					}
				}
				// Varios paréntesis --> insert of concrete columns
				else {
				
					String firstBracket = DMOsql.substring(indexFirstLeftBracket + 1, indexFirstRightBracket);
					
					String secondBracket = DMOsql.substring(indexSecondLeftBracket + 1, indexSecondRightBracket);
				
					String[] arrayColumns = firstBracket.split(",");
					String[] arrayValues = secondBracket.split(",");

					
					TableRef targetTableRef = refLog.getTableRef(targetSchemaVersion, targetTableName);
					Table targetTable = catalog.getTable(targetTableRef.getRefId());
					
					resultado.put("targetTable", targetTable);
					resultado.put("type", "concreteColumns");
					
					i = 0;
					while (i < arrayValues.length) {
						
						resultado.put(targetTable.getColumn(arrayColumns[i]), arrayValues[i]);
						i++;
					}
				}

			}
		}
		return resultado;
	}

	public String auxOperation(JoinPoint joinPoint) {
		String methodName = joinPoint.getSignature().getName(), fullClassName, className = null;
		int aux;
		String aux2 = null;
	
		if (schemaOperation) {
			fullClassName = joinPoint.getTarget().getClass().getName();
			className = fullClassName.substring(fullClassName.lastIndexOf(".") + 1);
		
			aux = className.lastIndexOf("Migrator");
			aux2 = className.substring(0, aux);
			
			return aux2;
		} else {
			
			return DMOsql;
		}
	}


	private String auxColumnValueID(ArrayList<String> keyValues, String idColumn, String tableName, String columnName) {

		return idColumn + "_" + columnName + "_" + auxRowID(keyValues);

	}


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

	
		if (schemaOperation)
			return "smo";
		else
			return "dmo";
	}

	private void provStartOperation(JoinPoint joinPoint, String ID_MSG, String operation, String operatorType,
			String start) {
		Identified target = null;
		
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();

		String aux = operation, operator = aux;
		if (aux.indexOf(" ") != -1)
			operator = aux.substring(0, aux.indexOf(" "));
		
		provenance(EXECUTION_ID, target, ID_MSG, auxOperationType(), ID_MSG, operatorType);
		provenanceValueType(target, "operatorName", EXECUTION_ID, ID_MSG, completeID, "type", operator, operatorType);
		provenanceValueType(target, "startTransTime", EXECUTION_ID, ID_MSG, completeID, "startTransTime", start,
				operatorType);
		provenanceValueType(target, "operationInstruction", EXECUTION_ID, ID_MSG, completeID, "instruction", operation,
				operatorType);

	}

	private void provSuccessEndOperation(JoinPoint joinPoint, String ID_MSG, String operation, String operatorType,
			String start) {
	
		Identified target = null;
	
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();

		provStartOperation(joinPoint, ID_MSG, operation, operatorType, start);
		provenanceValueType(target, "executed", EXECUTION_ID, ID_MSG, completeID, "executed", "true", operatorType);
		provenanceValueType(target, "endTransTime", EXECUTION_ID, ID_MSG, completeID, "endTransTime",
				String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);

	}

	private void provErrorEndOperation(JoinPoint joinPoint, String ID_MSG, String operation, String operatorType,
			String start) {
		
		Identified target = null;
		
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();

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
		
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		
		provenance(EXECUTION_ID, target, ID_MSG, varEntityName, completeID, operatorType);
		provenanceValueType(target, varAttributeName, EXECUTION_ID, ID_MSG, completeID, "value", value, operatorType);
	}


	private void provComplexColumn(JoinPoint joinPoint, String ID_MSG, String columnName, String columnType,
			String nameVar, String ID_COL, String linkedVar, String linkedValue, String operatorType) {
		String prefix1 = "", prefix2 = "";
		Identified target = null;
		
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
		
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		
		provenance(EXECUTION_ID, target, ID_MSG, varName, ID_COL, operatorType);
		if (varName.equalsIgnoreCase("sourColumn")) {
			prefix = "s";
		} else if (varName.equalsIgnoreCase("inputColumnDef")) {
			prefix = "in";
		}

		provenanceValueType(target, prefix + "ColumnName", EXECUTION_ID, ID_MSG, completeID, "columnName", columnName,
				operatorType);
		if (columnType != null)
			provenanceValueType(target, prefix + "ColumnType", EXECUTION_ID, ID_MSG, completeID, "typeName", columnType,
					operatorType);
		
	}

	private void provSimpleColumnValue(JoinPoint joinPoint, String ID_MSG, String columnValueVar, String columnName,
			String columnValue, String ID_COL_Value, String rowId, String operatorType) {
		Identified target = null;
		String prefix = "";
		
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		provenance(EXECUTION_ID, target, ID_MSG, columnValueVar, ID_COL_Value, operatorType);
		if (columnValueVar.equalsIgnoreCase("sourceColumnValue")) {
			prefix = "s";
		} else if (columnValueVar.equalsIgnoreCase("value")) {
			prefix = "v";
		}
		provenanceValueType(target, prefix + "ColumnValue", EXECUTION_ID, ID_MSG, completeID, "columnValue",
				columnValue, operatorType);
		provenanceValueType(target, prefix + "RowId", EXECUTION_ID, ID_MSG, completeID, "rowID", rowId, operatorType);
	}

	private void provSimplestColumnValue(JoinPoint joinPoint, String ID_MSG, String columnValueVar, String value,
			String ID_COL_Value, String operatorType) {
		Identified target = null;
		
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
		
		Identified target = null;
		
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID();
		
		Connection con = null;
		PreparedStatement ps = null;
		ResultSetMetaData rsM = null;
		ResultSet rs = null;
		String prefix = "", prefix2 = "";
		String jdbcUrl = null;
		String jdbcUser = null;
		String jdbcPass = null;
		if (conf != null) {
			jdbcUrl = conf.getUrl();

			jdbcUser = conf.getUser();

			jdbcPass = conf.getPassword();
		}
		Set<Object> columnasTarget = null;
		List<Object> columnasTargetList = new ArrayList<Object>();
		
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
		
		ArrayList<String> keyValues = new ArrayList<String>();
		ArrayList<Column> keyColumns = new ArrayList<Column>();
		
		int i = 1, numColumns = 0;
		int numKeyColumns = 0;
		String value = null;
		
		String keyColumnsSelect = "";
		boolean entra = false;
		
		if (insertSelect) {
			try {
				con = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);

				sourceTableRef = (TableRef) valoresDMOmethod.get("sourceTable");
				targetTableRef = (TableRef) valoresDMOmethod.get("targetTable");
				
				targetTable = catalog.getTable(targetTableRef.getRefId());
				
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
				
				valoresDMOmethod.remove("sourceTable");
				valoresDMOmethod.remove("targetTable");
				
				if (schemaOperation)
					ps = con.prepareStatement("select " + keyColumnsSelect + ", * from " + sourceTable.getName()
							+ " order by row_number() over() desc limit " + numberSelectCount);
				else
				
					ps = con.prepareStatement("select " + keyColumnsSelect + ", * from " + targetTable.getName()
							+ " order by row_number() over() desc limit " + numberSelectCount);
				rs = ps.executeQuery();
				rsM = ps.getMetaData();
				numColumns = rsM.getColumnCount();
			

				columnasTarget = (Set<Object>) valoresDMOmethod.keySet();
				
				columnasTargetList.addAll(columnasTarget);
				

				targetColumn = null;
				sourceColumn = null;
				int contadorFilas = 0;
				while (rs.next()) {
					contadorFilas = 1;
					
					keyValues.clear();
					for (int j = 1; j <= numKeyColumns; j++) {
						keyValues.add(rs.getObject(j).toString());

					}
					i = numKeyColumns + 1;
					while (i <= numColumns) {
					
						columnValue = rs.getObject(i).toString();

						targetColumn = (Column) columnasTargetList.get(i - 1 - numKeyColumns);
						
						/////////////////////////////// Associated Columns in target
						idTargetColumnUUID = new UUID(targetColumn);
						
						idTargetColumn = "Column" + targetColumn.hashCode() + "_" + targetTableRef.getName();
						targetColumnName = targetColumn.getName();
						//////////////////////////////////
						////////////////////////////// Source Values in source
						sourceColumn = (Column) valoresDMOmethod.get(targetColumn);

						sourceColumnName = sourceColumn.getName();
						
						idSourceColumnUUID = new UUID(sourceColumn);
						
						idSourceColumn = "Column" + sourceColumn.hashCode() + "_" + sourceTable.getName();
						columnValue = columnValue.replace("'", "");
					
						if (contadorFilas == 1) {
							
							if (columnValueVar.equalsIgnoreCase("newColumnValue")) {
								
								provSimpleColumn(joinPoint, ID_MSG, targetColumnName, columnVar, null, idTargetColumn,
										operatorType);

							}
						}
					

						
						sourceColumnValueId = auxColumnValueID(keyValues, idSourceColumn, sourceTable.getName(),
								sourceColumnName);
						
						if (columnValueVar.equalsIgnoreCase("newColumnValue")) {

							provSimpleColumnValue(joinPoint, ID_MSG, "value", sourceColumnName, columnValue,
									sourceColumnValueId, auxRowID(keyValues), operatorType);
						} else if (columnValueVar.equalsIgnoreCase("targetColumnValue")) {
							provSimpleColumnValue(joinPoint, ID_MSG, "sourceColumnValue", sourceColumnName, columnValue,
									sourceColumnValueId, auxRowID(keyValues), operatorType);
						}

						
						targetColumnValueId = auxColumnValueID(keyValues, idTargetColumn, targetTable.getName(),
								targetColumnName);
						
						provenance(EXECUTION_ID, target, ID_MSG, columnValueVar, targetColumnValueId, operatorType);
						if (columnValueVar.equalsIgnoreCase("targetColumnValue")) {
							prefix = "t";
							prefix2 = "tcv";
						} else if (columnValueVar.equalsIgnoreCase("newColumnValue")) {
							prefix = "nv";
							prefix2 = prefix;
						}
						provenanceValueType(target, prefix2 + "StartTransTime", EXECUTION_ID, ID_MSG, completeID,
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
		

				}
				ps.close();

			} catch (SQLException e) {
			
				e.printStackTrace();
			}
		}
		// Usual insert
		else {
			targetTableRef = (TableRef) valoresDMOmethod.get("targetTable");
			targetTable = catalog.getTable(targetTableRef.getRefId());
			keyColumns = (ArrayList<Column>) targetTable.getPrimaryKeyColumns();
			
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
				

				targetColumn = (Column) columnasInsertList.get(i - 1);
				targetColumnName = targetColumn.getName();
				

				columnValue = (String) valoresDMOmethod.get(targetColumn);
				
				/////////////////////////////// Associated Columns in target
				idTargetColumnUUID = new UUID(targetColumn);
				idTargetColumn = String.valueOf(targetColumn.hashCode());
				
				provSimpleColumn(joinPoint, ID_MSG, targetColumnName, columnVar, null, idTargetColumn, operatorType);

			
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
			
				value = value.replace("'", "");
				provSimplestColumnValue(joinPoint, ID_MSG, "value", value, String.valueOf(value.hashCode()),
						operatorType);
				i++;

			}
		}
	}

	private void provAuthor(JoinPoint joinPoint, String ID_MSG, String author, String operatorType) {
		Identified target = null;
		
		if (schemaOperation) {
			target = (Identified) joinPoint.getTarget();
		} else
			target = (Identified) joinPoint.getThis();

		
		provenance(EXECUTION_ID, target, ID_MSG, "user", author, operatorType);

	}

	private void provenance(String ID_EXECUTION, final Identified target, String ID_EXECUTION_METHOD,
			String variableName, String value, String operatorType) {
		try {
			
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

		
			BGMEvent e = new BGMEvent(target, ID_EXECUTION, operatorType, ID_EXECUTION_METHOD, variableName, value);
			bgmm.fireEvent("newValueBinding", e);
		} catch (InvocationTargetException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

}