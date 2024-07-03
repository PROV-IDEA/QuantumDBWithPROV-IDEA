package prov.idea.instrumenter;

import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
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
import io.quantumdb.core.schema.operations.DropTable;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.schema.operations.DataOperation;
import io.quantumdb.core.schema.operations.Operation;
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
	@DeclareParents(value = "io.quantumdb.core.migration.operations.DropTableMigrator", defaultImpl = IdentifiersManager.class)
	public static Identified ident7;

	public static EventHelper<BGMEventListener> bgmm = new EventHelper<>(BGMEventListener.class);
	public static EventHelper<BGMEventListener> nestedBgmm = new EventHelper<>(BGMEventListener.class);
	static {

	}
	private String DMOsql = null;
	private Config conf = null;
	private String author = null;

	private boolean insertSelect = false;
	private boolean insertSelectUnion = false;

	private boolean previousCopy = false;
	private boolean previousMerge = false, previousDecompose = false;
	private boolean schemaOperation = false;
	private boolean previousComplexSchemaOperation = false;
	private String previousComplexSchemaOperationName = null;
	private JoinPoint previousComplexSchemaOperationJoinPoint = null;
	private String previousComplexSchemaOperationID_MSG = null;
	private String superOperationType = null;
	private JoinPoint superJoinPoint = null;
	String sourceTable1Name = null, sourceTable2Name = null;
	String sourceTableNameDecom = null;
	TableRef targetTable1Ref = null, targetTable2Ref = null;
	Version sourceSchema1Version = null, targetSchema1Version = null, version1 = null, version2 = null;
	String sourceSchema1VersionID = null, targetSchema1VersionID = null, sourceSchema2VersionID = null,
			targetSchema2VersionID = null;
	HashMap<Object, Object> valoresDMODecompose1 = null, valoresDMODecompose2 = null;
	HashMap<Object, Object> columnasTargetSource1 = null;
	HashMap<Object, Object> columnasTargetSource2 = null;
	HashMap<String, Object> changeSetWithAssoOperations = new HashMap<String, Object>();

	boolean previousInsertDecompose = false;
	String forkId = null;
	String dropTableName = null;
	String dropTable1Name = null, dropTable2Name = null;
	int lastRowId = 0;

	int numberSelectCount = 0, numberSelectCountUnionSecondTable = 0;
	int targetSchemaNumber = 0;

	private static final String EXECUTION_ID = IdentifiersManager.randomUUID();

	@Pointcut("execution(* io.quantumdb.core.versioning.Changelog.addChangeSet(..))")
	public void changesetOperations() {
	}

	@Pointcut("execution (* io.quantumdb.core.migration.Migrator.migrate(..))")
	public void fork() {
	}

	@Pointcut("execution (io.quantumdb.core.backends.Config.new(..))")
	public void connection() {
	}

	@Pointcut("execution(* io.quantumdb.core.versioning.ChangeSet.getId())")
	public void changesetIdToPROV() {
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

	@Pointcut("execution(* io.quantumdb.core.migration.operations.DropTableMigrator.migrate(..))")
	public void dropTableToPROV() {
	}

	@Pointcut("execution (io.quantumdb.core.schema.operations.DataOperation.new(..))")
	public void catchSQLInDMO() {
	}

	@Pointcut("call(* io.quantumdb.core.backends.DatabaseMigrator+.applyDataChanges(io.quantumdb.core.versioning.State,io.quantumdb.core.migration.Migrator.Stage))")
	public void insertDMOToPROV() {
	}

	@Before("changesetOperations()")
	public void changesetOperations(JoinPoint joinPoint) {
		DataOperation dop = null;
		List<String> auxListSQLOperations = new ArrayList<String>();
		List<String> ope = new ArrayList<String>();
		Object[] signatureArgs = joinPoint.getArgs();
		Object changeSetId = signatureArgs[0];
		String changeSetIdString = null;
		if (changeSetId instanceof io.quantumdb.core.versioning.Version)
			changeSetIdString = ((Version) changeSetId).getChangeSet().getId();
		else if (changeSetId instanceof String)
			changeSetIdString = (String) changeSetId;
		boolean alreadyRegistered = false;
		Set<String> keySet = changeSetWithAssoOperations.keySet();

		if (changeSetWithAssoOperations.containsKey(changeSetIdString))
			alreadyRegistered = true;
		if (!changeSetIdString.equalsIgnoreCase("initial") && !alreadyRegistered) {
			if (signatureArgs.length == 4) {
				Collection<Operation> operations = (Collection<Operation>) signatureArgs[3];
				for (Operation o : operations) {
					if (o instanceof io.quantumdb.core.schema.operations.DataOperation) {
						dop = (DataOperation) o;
						auxListSQLOperations.add(dop.getQuery());
					}
				}
				changeSetWithAssoOperations.put(changeSetIdString, auxListSQLOperations);
			} else if (signatureArgs.length == 3) {
				Collection<Operation> operations = (Collection<Operation>) signatureArgs[2];
				for (Operation o : operations) {
					if (o instanceof io.quantumdb.core.schema.operations.DataOperation) {
						dop = (DataOperation) o;
						auxListSQLOperations.add(dop.getQuery());
					}
				}
				changeSetWithAssoOperations.put(changeSetIdString, auxListSQLOperations);
			} else if (signatureArgs.length == 2) {
				Collection<Operation> operations = (Collection<Operation>) signatureArgs[1];
				for (Operation o : operations) {
					if (o instanceof io.quantumdb.core.schema.operations.DataOperation) {
						dop = (DataOperation) o;
						auxListSQLOperations.add(dop.getQuery());
					}
				}
				changeSetWithAssoOperations.put(changeSetIdString, auxListSQLOperations);
			}
		}
	}

	@Before("fork()")
	public void forkInvoca(JoinPoint joinPoint) {
		Object[] signatureArgs = joinPoint.getArgs();
		State signatureArg = (State) signatureArgs[0];
		Changelog changelog = signatureArg.getChangelog();
		Version from = (Version) changelog.getVersion((String) signatureArgs[2]);
		forkId = from.getChangeSet().getId();
		inicializaVariablesEntreChangeSets();
		checkSuperOperationType(forkId, joinPoint);

	}

	public void inicializaVariablesEntreChangeSets() {
		insertSelect = false;
		insertSelectUnion = false;
		previousCopy = false;
		previousMerge = false;
		previousDecompose = false;
		schemaOperation = false;
		previousComplexSchemaOperation = false;
		previousComplexSchemaOperationName = null;
		previousComplexSchemaOperationJoinPoint = null;
		previousComplexSchemaOperationID_MSG = null;
		superOperationType = null;
		superJoinPoint = null;
		sourceTable1Name = null;
		sourceTable2Name = null;
		sourceTableNameDecom = null;
		targetTable1Ref = null;
		targetTable2Ref = null;
		sourceSchema1Version = null;
		targetSchema1Version = null;
		version1 = null;
		version2 = null;
		sourceSchema1VersionID = null;
		targetSchema1VersionID = null;
		sourceSchema2VersionID = null;
		targetSchema2VersionID = null;
		valoresDMODecompose1 = null;
		valoresDMODecompose2 = null;
		columnasTargetSource1 = null;
		columnasTargetSource2 = null;
		lastRowId = 0;
		numberSelectCount = 0;
		numberSelectCountUnionSecondTable = 0;
	}

	public void checkSuperOperationType(String forkId, JoinPoint joinPoint) {
		String aux0 = forkId;
		String aux = aux0.substring(0, aux0.indexOf("-") + 1);
		String aux2 = forkId.substring(forkId.indexOf("-") + 1, forkId.length());
		String aux3 = null;
		if (aux2.indexOf("-") != -1)
			aux3 = aux2.substring(0, aux2.indexOf("-"));
		else
			aux3 = aux2;
		String tablas = null;
		if (aux2.indexOf("-") != -1)
			tablas = aux2.substring(aux2.indexOf("-"), aux2.length());// tablas con el guion delante
		targetSchemaNumber = Integer.parseInt(aux3);
		if (forkId.indexOf("merge") != -1) {
			superOperationType = "MergeTable";
			sourceTable1Name = tablas.substring(tablas.indexOf("-") + 1, tablas.lastIndexOf("-"));
			sourceTable2Name = tablas.substring(tablas.lastIndexOf("-") + 1, tablas.length());
			superJoinPoint = joinPoint;
		} else if (forkId.indexOf("decom") != -1) {
			superOperationType = "DecomposeTable";
			sourceTableNameDecom = tablas.substring(tablas.indexOf("-") + 1, tablas.length());
			superJoinPoint = joinPoint;
		}
	}

	@Before("catchSQLInDMO()")
	public void logCatchSQLQuery(JoinPoint joinPoint) {
		Object[] signatureArgs = joinPoint.getArgs();
		String DMOsqlAux = ((String) signatureArgs[0]).trim();
		DMOsql = DMOsqlAux;
	}

	public void obtenRowIDAndDMOsql(int i, boolean drop) {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String jdbcUrl = conf.getUrl();
		String jdbcUser = conf.getUser();
		String jdbcPass = conf.getPassword();
		String sourceTableName = null, sourceTable2Name = null;
		String targetTableName = null;
		String select = null;
		String select2 = null;
		List<String> listSql = (List<String>) changeSetWithAssoOperations.get(forkId);
		if (forkId.contains("drop") || drop) {
			try {
				DMOsql = "Select * from " + dropTableName + ";";
				con = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
				ps = con.prepareStatement("select row_number() over() from " + dropTableName
						+ " order by row_number() over() desc limit 1");
				rs = ps.executeQuery();
				if (rs.next()) {
					lastRowId = rs.getInt(1);
				}
				rs.close();
				ps.close();
			} catch (SQLException e) {
				lastRowId = 0;
			} finally {
				try {
					if (con != null)
						con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		} else {
			DMOsql = listSql.get(i);
			int indexSelect = DMOsql.indexOf("select");
			int indexUnion = DMOsql.indexOf("union");
			if (indexSelect != -1) {
				insertSelect = true;
				String[] palabras = DMOsql.split("\\s+");
				if (indexUnion == -1) {
					if (palabras[4].equals("*")) {
						sourceTableName = palabras[6].substring(0, palabras[6].length() - 1);
					} else {
						int leng = palabras.length - 1;
						sourceTableName = palabras[leng].substring(0, palabras[leng].length() - 1);
					}

				} else {
					insertSelectUnion = true;
					sourceTableName = palabras[6].substring(0, palabras[6].length());
					sourceTable2Name = palabras[11].substring(0, palabras[11].length() - 2);
				}
				targetTableName = palabras[2];
				select = "select count(*) from " + sourceTableName;
				select2 = null;
				if (insertSelectUnion)
					select2 = "select count(*) from " + sourceTable2Name;
			}
			try {

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
					lastRowId = 0;
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
					numberSelectCount = 0;
				}
				try {
					if (insertSelectUnion) {
						ps = con.prepareStatement(select2);
						rs = ps.executeQuery();
						if (rs.next()) {
							numberSelectCountUnionSecondTable = rs.getInt(1);
						}
						rs.close();
						ps.close();
					}

				} catch (SQLException e) {
					numberSelectCountUnionSecondTable = 0;
				}
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

	@AfterThrowing("createTableToPROV() || dropTableToPROV() || copyTableToPROV() || insertDMOToPROV()")
	public void logCreateTableError(JoinPoint joinPoint) throws Exception {
		UUID id = IdentifiersManager.getIdentifier(joinPoint.getTarget());
		final String ID_MSG = id.getUUID();
		String operatorType = auxOperation(joinPoint);
		if (!operatorType.equals("CreateTable") && !operatorType.equals("DropTable")
				&& !operatorType.equals("CopyTable"))
			operatorType = "Insert";
		String start = String.valueOf(new Timestamp(System.currentTimeMillis()));
		provErrorEndOperation(joinPoint, ID_MSG, operatorType, operatorType, start);
	}

	@After("createTableToPROV() || dropTableToPROV() || copyTableToPROV() || insertDMOToPROV() ")
	public void logOperations(JoinPoint joinPoint) {
		String operationType = null;
		boolean enter = false;
		UUID idUUID = IdentifiersManager.getIdentifier(joinPoint.getTarget());
		String ID_MSG = idUUID.getUUID();
		RefLog refLog = null;
		Version version = null;
		Changelog changelog = null;
		String sourceTableName = null;
		String targetTableName = null;
		String previousName = null;
		Version sourceSchemaVersion = null, sourceSchema2Version = null, targetSchemaVersion = null,
				targetSchema2Version = null;
		String sourceSchemaVersionID = null, targetSchemaVersionID = null;
		String catalogName = null;
		Catalog catalog = null;
		State state = null;
		Stage stage = null;
		UUID idColumnUUID = null;
		String idColumn = null;
		TableRef sourceTableRef = null, targetTableRef = null, sourceTable1Ref = null, sourceTable2Ref = null;
		Table sourceTable = null, sourceTable1 = null, sourceTable2 = null, targetTable = null, targetTable1 = null,
				targetTable2 = null;
		ImmutableList<ColumnDefinition> columnsDef = null;
		ImmutableList<Column> columns = null;
		Object[] signatureArgs = joinPoint.getArgs();
		HashMap<Object, Object> valoresDMO = null;
		schemaOperation = false;
		for (Object signatureArg : signatureArgs) {
			if (signatureArg instanceof io.quantumdb.core.schema.operations.SchemaOperation) {
				schemaOperation = true;
				if (signatureArg instanceof io.quantumdb.core.schema.operations.CreateTable) {
					if (superOperationType != null && superOperationType.equalsIgnoreCase("MergeTable")) {
						previousComplexSchemaOperation = true;
						previousComplexSchemaOperationName = "MergeTable";
						previousComplexSchemaOperationJoinPoint = joinPoint;
						operationType = "MergeTable";
						targetTableName = ((CreateTable) signatureArg).getTableName();
						String ID_MSGAux = idUUID.getUUID();
						ID_MSGAux = ID_MSGAux.replaceAll("Create", "Merge");
						BigInteger b = new BigInteger(EXECUTION_ID);
						BigInteger b2 = b.add(new BigInteger("1"));
						String EXECUTION_ID_CREATE = String.valueOf(b2);
						ID_MSG = ID_MSGAux.replaceAll(EXECUTION_ID_CREATE, EXECUTION_ID);
						previousComplexSchemaOperationID_MSG = ID_MSG;
					} else if (superOperationType != null && superOperationType.equalsIgnoreCase("DecomposeTable")) {
						operationType = "DecomposeTable";
						previousComplexSchemaOperation = true;
						previousComplexSchemaOperationName = "DecomposeTable";
						previousComplexSchemaOperationJoinPoint = joinPoint;
						operationType = "DecomposeTable";
						String ID_MSGAux = idUUID.getUUID();
						ID_MSGAux = ID_MSGAux.replaceAll("Create", "Decompose");
						BigInteger b = new BigInteger(EXECUTION_ID);
						BigInteger b2 = b.add(new BigInteger("1"));
						String EXECUTION_ID_CREATE = String.valueOf(b2);
						ID_MSG = ID_MSGAux.replaceAll(EXECUTION_ID_CREATE, EXECUTION_ID);
						previousComplexSchemaOperationID_MSG = ID_MSG;
					} else
						operationType = "CreateTable";
					targetTableName = ((CreateTable) signatureArg).getTableName();
					columnsDef = ((CreateTable) signatureArg).getColumns();
				}
				if (signatureArg instanceof io.quantumdb.core.schema.operations.CopyTable) {
					previousComplexSchemaOperation = true;
					previousComplexSchemaOperationName = "CopyTable";
					previousComplexSchemaOperationJoinPoint = joinPoint;
					previousComplexSchemaOperationID_MSG = ID_MSG;
					operationType = "CopyTable";
					sourceTableName = ((CopyTable) signatureArg).getSourceTableName();
					targetTableName = ((CopyTable) signatureArg).getTargetTableName();
				}
				if (signatureArg instanceof io.quantumdb.core.schema.operations.DropTable) {
					operationType = "DropTable";
					sourceTableName = ((DropTable) signatureArg).getTableName();
					dropTableName = sourceTableName;
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
		if (!operationType.equalsIgnoreCase("dmo")) {
			catalogName = catalog.getName();
			if (operationType.equalsIgnoreCase("CreateTable")
					|| (superOperationType != null && superOperationType.equalsIgnoreCase("DecomposeTable"))
							&& !operationType.equalsIgnoreCase("DropTable")) {
				if (superOperationType != null && superOperationType.equalsIgnoreCase("DecomposeTable")) {
					if (targetTable1Ref == null) {
						obtenRowIDAndDMOsql(0, false);
						targetTable1Ref = refLog.getTableRef(version, targetTableName);
						sourceSchema1Version = version.getParent();
						targetSchema1Version = version;
						sourceSchema1VersionID = version.getParent().getId();
						targetSchema1VersionID = version.getId();
						version1 = version;
						valoresDMODecompose1 = auxProcessDMO(DMOsql, catalog, refLog, sourceSchema1Version,
								targetSchema1Version);

					} else if (targetTable1Ref != null) {
						obtenRowIDAndDMOsql(1, false);
						targetTable2Ref = refLog.getTableRef(version, targetTableName);
						sourceSchema2Version = version.getParent();
						targetSchema2Version = version;
						sourceSchema2VersionID = version.getParent().getId();
						targetSchema2VersionID = version.getId();
						version2 = version;
						valoresDMODecompose2 = auxProcessDMO(DMOsql, catalog, refLog, sourceSchema2Version,
								targetSchema2Version);
					}

				}
			} else {
				obtenRowIDAndDMOsql(0, false);
				sourceSchemaVersion = version.getParent();
				targetSchemaVersion = version;
				sourceSchemaVersionID = version.getParent().getId();
				targetSchemaVersionID = version.getId();
			}
			if (operationType.equalsIgnoreCase("DropTable") && ((superOperationType == null)
					|| (superOperationType != null && !superOperationType.equalsIgnoreCase("DecomposeTable"))
							&& (superOperationType != null && !superOperationType.equalsIgnoreCase("MergeTable")))) {
				sourceTableRef = refLog.getTableRef(sourceSchemaVersion, sourceTableName);
			} else if (!operationType.equalsIgnoreCase("DropTable") && (superOperationType == null
					|| (superOperationType != null && !superOperationType.equalsIgnoreCase("DecomposeTable")))) {
				targetTableRef = refLog.getTableRef(version, targetTableName);
				targetTable = catalog.getTable(targetTableRef.getRefId());
			}

			if (operationType.equalsIgnoreCase("CopyTable")) {
				sourceTableRef = refLog.getTableRef(sourceSchemaVersion, sourceTableName);
				sourceTable = catalog.getTable(sourceTableRef.getRefId());
				columns = targetTable.getColumns();
			}
			if (operationType.equalsIgnoreCase("MergeTable") && !operationType.equalsIgnoreCase("DropTable")) {

				sourceTable1Ref = refLog.getTableRef(sourceSchemaVersion, sourceTable1Name);
				sourceTable2Ref = refLog.getTableRef(sourceSchemaVersion, sourceTable2Name);
				sourceTable1 = catalog.getTable(sourceTable1Ref.getRefId());
				sourceTable2 = catalog.getTable(sourceTable2Ref.getRefId());
				columns = targetTable.getColumns();
			}
			if (operationType.equalsIgnoreCase("DecomposeTable") && !operationType.equalsIgnoreCase("DropTable")) {
				sourceTableRef = refLog.getTableRef(sourceSchema1Version, sourceTableNameDecom);
				sourceTable = catalog.getTable(sourceTableRef.getRefId());
			}
			if ((superOperationType == null || !superOperationType.equalsIgnoreCase("DecomposeTable"))
					&& !operationType.equalsIgnoreCase("DropTable")) {
				valoresDMO = auxProcessDMO(DMOsql, catalog, refLog, sourceSchemaVersion, targetSchemaVersion);
			} else if (operationType.equalsIgnoreCase("DropTable") && ((superOperationType == null)
					|| (superOperationType != null && !superOperationType.equalsIgnoreCase("MergeTable")))) {
				sourceTableRef = refLog.getTableRef(version.getParent(), dropTableName);
				sourceTable = catalog.getTable(sourceTableRef.getRefId());
				valoresDMO = new HashMap<Object, Object>();
				valoresDMO.put(dropTableName, sourceTable);
			}
		} else {
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
			} else {
				targetTableRef = (TableRef) valoresDMO.get("targetTable");
				targetTableName = targetTableRef.getName();
				Set<Object> objectsSet = valoresDMO.keySet();
				targetTable = catalog.getTable(targetTableRef.getRefId());
				columns = targetTable.getColumns();
				Set<Column> columnsSet = new HashSet<Column>();
			}
		}
		/********************* Generating entities ****************/
		/**********************************************************/
		if ((operationType.equalsIgnoreCase("CreateTable")
				|| (superOperationType != null && superOperationType.equalsIgnoreCase("DecomposeTable")))
				&& !operationType.equalsIgnoreCase("dmo") && !operationType.equalsIgnoreCase("DropTable")) {
			if (superOperationType != null && superOperationType.equalsIgnoreCase("DecomposeTable")) {
				if (targetTable2Ref != null) {
					decomposeToProv(superJoinPoint, previousComplexSchemaOperationID_MSG, catalog, refLog, version1,
							sourceSchema1VersionID, targetSchema2VersionID, sourceTableRef, targetTable1Ref,
							targetTable2Ref);
				}
			} else {
				createToProv(joinPoint, ID_MSG, catalog, refLog, version, sourceSchemaVersionID, targetSchemaVersionID,
						targetTableRef, true);
			}
		} else if (operationType.equalsIgnoreCase("MergeTable")) {
			mergeToProv(superJoinPoint, ID_MSG, catalog, refLog, version, sourceSchemaVersionID, targetSchemaVersionID,
					sourceTable1Ref, sourceTable2Ref, targetTableRef, valoresDMO);
		} else if (operationType.equalsIgnoreCase("DropTable") && ((superOperationType == null)
				|| (superOperationType != null && !superOperationType.equalsIgnoreCase("DecomposeTable"))
						&& (superOperationType != null && !superOperationType.equalsIgnoreCase("MergeTable")))) {
			dropToProv(joinPoint, ID_MSG, catalog, refLog, version, sourceSchemaVersionID, targetSchemaVersionID,
					sourceTableRef, valoresDMO);
		} else if (operationType.equalsIgnoreCase("CopyTable")) {
			copyToProv(joinPoint, ID_MSG, catalog, refLog, version, sourceSchemaVersionID, targetSchemaVersionID,
					sourceTableRef, targetTableRef, valoresDMO);
		} else if (operationType.equalsIgnoreCase("dmo")) {
			if (previousComplexSchemaOperation) {
				final String nested_create_ID_MSG = previousComplexSchemaOperationID_MSG + "Insert";
				if (!previousInsertDecompose) {
					DMOToProv(previousComplexSchemaOperationJoinPoint, nested_create_ID_MSG + "1", catalog,
							targetTableRef, valoresDMO);
					previousInsertDecompose = true;
				} else
					DMOToProv(previousComplexSchemaOperationJoinPoint, nested_create_ID_MSG + "2", catalog,
							targetTableRef, valoresDMO);
			} else {
				DMOToProv(joinPoint, ID_MSG, catalog, targetTableRef, valoresDMO);
			}
		}
		/**********************************************************/
		/************************ End operator ********************/
		/**********************************************************/
		String operatorType = auxOperation(joinPoint);
		if (!operatorType.equals("CreateTable") && !operatorType.equals("CopyTable")
				&& !operatorType.equals("MergeTable") && !operatorType.equals("DecomposeTable"))
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
		provAuthor(joinPoint, ID_MSG, author, operation);
		provInputValue(joinPoint, ID_MSG, "inputTableName", "inTableName", targetTableName, operatorType);
		provSourceSchema(joinPoint, ID_MSG, sourceSchemaVersionID, catalogName, operatorType);
		provTargetSchema(joinPoint, ID_MSG, targetSchemaVersionID, catalogName, operatorType);
		provComplexTable(joinPoint, ID_MSG, "targetTable", targetTableRef.getRefId(), targetTableName, operatorType);
		// Target table's columns
		for (Column col : columns) {
			idColumn = auxColumnID(targetSchemaNumber, targetTableName, col.getName());
			provSimpleColumn(joinPoint, ID_MSG, col.getName(), "inputColumnDef", col.getType().toString(), idColumn,
					operatorType);
			provComplexColumn(joinPoint, ID_MSG, col.getName(), col.getType().toString(), "targetColumn", idColumn,
					"inputColumnDef", idColumn, operatorType);
		}
		// Previous tables
		ImmutableSet<Table> tables = catalog.getTables();
		boolean decomposeFirstTable = ID_MSG.endsWith("Create1");
		boolean decomposeSecondTable = ID_MSG.endsWith("Create2");
		for (Table t : tables) {
			previousName = t.getName();
			if (!previousName.equalsIgnoreCase(targetTableRef.getRefId())) {
				if (decomposeFirstTable && !previousName.equalsIgnoreCase(targetTable2Ref.getRefId())) {
					provSimpleTable(joinPoint, ID_MSG, "previousTable", previousName, previousName, operatorType);
				} else if (decomposeSecondTable && previousName.equalsIgnoreCase(targetTable1Ref.getRefId())) {
					provSimpleTable(joinPoint, ID_MSG, "previousTable", targetTable1Ref.getRefId(), previousName,
							operatorType);
				} else if (decomposeSecondTable && !previousName.equalsIgnoreCase(targetTable2Ref.getRefId())) {
					provSimpleTable(joinPoint, ID_MSG, "previousTable", previousName, previousName, operatorType);
					// Usual create
				} else if (!decomposeFirstTable && !decomposeSecondTable) {
					provSimpleTable(joinPoint, ID_MSG, "previousTable",
							refLog.getTableRef(targetVersion.getParent(), previousName).getRefId(), previousName,
							operatorType);
				}
			}
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

	public void dropToProv(JoinPoint joinPoint, String ID_MSG, Catalog catalog, RefLog refLog, Version targetVersion,
			String sourceSchemaVersionID, String targetSchemaVersionID, TableRef sourceTableRef,
			HashMap<Object, Object> valoresDMO) {
		previousCopy = true;
		String operation = "DropTable";
		String operatorType = "DropTable";
		String catalogName = catalog.getName();
		String idColumnSource = null, idColumnTarget = null, previousName = null;
		String sourceTableName = sourceTableRef.getName();
		Table sourceTable = catalog.getTable(sourceTableRef.getRefId());
		ImmutableList<Column> columns = sourceTable.getColumns();
		UUID idColumnUUID = null;

		/**********************************************************/
		/*********************** Create listener ******************/
		/**********************************************************/

		SetBindingsListener lis = initiateListener(joinPoint, ID_MSG, operatorType);

		/**********************************************************/
		/************************ start operator ********************/
		/**********************************************************/

		String start = String.valueOf(new Timestamp(System.currentTimeMillis()));

		/**********************************************************/
		/************************ End operator ********************/
		/**********************************************************/

		provAuthor(joinPoint, ID_MSG, author, operatorType);
		provSourceSchema(joinPoint, ID_MSG, sourceSchemaVersionID, catalogName, operatorType);
		provTargetSchema(joinPoint, ID_MSG, targetSchemaVersionID, catalogName, operatorType);
		provComplexTable(joinPoint, ID_MSG, "sourceTable", sourceTableRef.getRefId(), sourceTableName, operatorType);
		// Source table's columns
		for (Column col : columns) {
			idColumnSource = auxColumnID(targetSchemaNumber - 1, sourceTableName, col.getName());
			provSimpleColumn(joinPoint, ID_MSG, col.getName(), "sourceColumn", null, idColumnSource, operatorType);
		}
		// Source columns' values
		provComplexColumnValues(joinPoint, ID_MSG, "sourceColumn", "sourceColumnValue", valoresDMO, catalog,
				operatorType);
		Set<Object> keys = (Set<Object>) valoresDMO.keySet();
		boolean haEntrado = false;
		TableRef targetTable1Ref = null, targetTable2Ref = null;
		for (Object o : keys) {
			if (o instanceof TableRef && !haEntrado) {
				targetTable1Ref = (TableRef) o;
				haEntrado = true;
			} else if (o instanceof TableRef && haEntrado) {
				targetTable2Ref = (TableRef) o;
			}
		}
		ImmutableSet<Table> tables = catalog.getTables();
		for (Table t : tables) {
			previousName = t.getName();
			if ((!previousName.equalsIgnoreCase(sourceTableRef.getRefId()))
					&& (dropTable1Name != null && !previousName.equalsIgnoreCase(dropTable1Name))) {
				if (dropTable2Name == null) {
					if (targetTable1Ref != null && previousName.equalsIgnoreCase(targetTable1Ref.getRefId()))
						provSimpleTable(joinPoint, ID_MSG, "previousTable",
								refLog.getTableRef(targetVersion.getParent(), (String) valoresDMO.get(targetTable1Ref))
										.getRefId(),
								previousName, operatorType);
					else if (targetTable2Ref != null && previousName.equalsIgnoreCase(targetTable2Ref.getRefId()))
						provSimpleTable(joinPoint, ID_MSG, "previousTable",
								refLog.getTableRef(targetVersion.getParent(), (String) valoresDMO.get(targetTable2Ref))
										.getRefId(),
								previousName, operatorType);
					else
						provSimpleTable(joinPoint, ID_MSG, "previousTable",
								refLog.getTableRef(targetVersion.getParent(), previousName).getRefId(), previousName,
								operatorType);
				} else if (dropTable2Name != null && !previousName.equalsIgnoreCase(dropTable2Name)) {
					if (targetTable1Ref != null && previousName.equalsIgnoreCase(targetTable1Ref.getRefId()))
						provSimpleTable(joinPoint, ID_MSG, "previousTable",
								refLog.getTableRef(targetVersion.getParent(), (String) valoresDMO.get(targetTable1Ref))
										.getRefId(),
								previousName, operatorType);
					else if (targetTable2Ref != null && previousName.equalsIgnoreCase(targetTable2Ref.getRefId()))
						provSimpleTable(joinPoint, ID_MSG, "previousTable",
								refLog.getTableRef(targetVersion.getParent(), (String) valoresDMO.get(targetTable2Ref))
										.getRefId(),
								previousName, operatorType);
					else
						provSimpleTable(joinPoint, ID_MSG, "previousTable",
								refLog.getTableRef(targetVersion.getParent(), previousName).getRefId(), previousName,
								operatorType);
				}
			}
		}

		provSuccessEndOperation(joinPoint, ID_MSG, operation, operatorType, start);
		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID, operatorType, ID_MSG);
		try {
			bgmm.fireEvent("operationEnd", initMethod);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
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
		provInformedByOperationCopy(joinPoint, ID_MSG);
		provAuthor(joinPoint, ID_MSG, author, operatorType);
		provInputValue(joinPoint, ID_MSG, "targetTableName", "inTableName", targetTableName, operatorType);
		provSourceSchema(joinPoint, ID_MSG, sourceSchemaVersionID, catalogName, operatorType);
		provTargetSchema(joinPoint, ID_MSG, targetSchemaVersionID, catalogName, operatorType);
		provSimpleTable(joinPoint, ID_MSG, "sourceTable", sourceTableRef.getRefId(), sourceTableName, operatorType);
		provComplexTable(joinPoint, ID_MSG, "targetTable", targetTableRef.getRefId(), targetTableName, operatorType);

		// Target columns' values
		provComplexColumnValues(joinPoint, ID_MSG, "targetColumn", "targetColumnValue", valoresDMO, catalog,
				operatorType);
		// Previous tables
		ImmutableSet<Table> tables = catalog.getTables();
		for (Table t : tables) {
			previousName = t.getName();
			if (!previousName.equalsIgnoreCase(targetTableRef.getRefId())) {
				provSimpleTable(joinPoint, ID_MSG, "previousTable",
						refLog.getTableRef(targetVersion.getParent(), previousName).getRefId(), previousName,
						operatorType);
			}
		}
		provSuccessEndOperation(joinPoint, ID_MSG, operation, operatorType, start);
		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID, operatorType, ID_MSG);
		try {
			bgmm.fireEvent("operationEnd", initMethod);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		bgmm.removeListener(lis);

	}

	public void decomposeToProv(JoinPoint joinPoint, String ID_MSG, Catalog catalog, RefLog refLog,
			Version targetVersion, String sourceSchemaVersionID, String targetSchemaVersionID, TableRef sourceTableRef,
			TableRef targetTable1Ref, TableRef targetTable2Ref) {

		previousDecompose = true;
		String operation = auxOperation(joinPoint);
		String operatorType = "DecomposeTable";
		String catalogName = catalog.getName();
		String idColumnSource = null, idColumnTarget = null, previousName = null;
		String sourceTableName = sourceTableRef.getName();
		String targetTable1Name = targetTable1Ref.getName();
		Table targetTable1 = catalog.getTable(targetTable1Ref.getRefId());
		String targetTable2Name = targetTable2Ref.getName();
		Table targetTable2 = catalog.getTable(targetTable2Ref.getRefId());
		Set<Object> columns1 = (Set<Object>) columnasTargetSource1.keySet();
		Set<Object> columns2 = (Set<Object>) columnasTargetSource2.keySet();
		UUID idColumnUUID = null;

		/**********************************************************/
		/************************ start operator ********************/
		/**********************************************************/

		String start = String.valueOf(new Timestamp(System.currentTimeMillis()));

		final String nested_create_ID_MSG = ID_MSG + "Create";
		createToProv(joinPoint, nested_create_ID_MSG + "1", catalog, refLog, version1, sourceSchema1VersionID,
				targetSchema1VersionID, targetTable1Ref, false);
		createToProv(joinPoint, nested_create_ID_MSG + "2", catalog, refLog, version2, sourceSchema2VersionID,
				targetSchema2VersionID, targetTable2Ref, false);

		/**********************************************************/
		/************************ End operator ********************/
		/**********************************************************/

		ID_MSG = ID_MSG.replaceAll("Create", "Decompose");
		BigInteger b = new BigInteger(EXECUTION_ID);
		BigInteger b2 = b.add(new BigInteger("1"));
		String EXECUTION_ID_CREATE = String.valueOf(b2);
		ID_MSG = ID_MSG.replaceAll(EXECUTION_ID_CREATE, EXECUTION_ID);
		SetBindingsListener lis = initiateListener(joinPoint, ID_MSG, operatorType);
		provInformedByOperationMergeDecompose(joinPoint, ID_MSG, "Decompose");
		provAuthor(joinPoint, ID_MSG, author, operatorType);
		provInputValue(joinPoint, ID_MSG, "targetTableName", "inTableName", targetTable1Name, operatorType);
		provInputValue(joinPoint, ID_MSG, "targetTableName", "inTableName", targetTable2Name, operatorType);
		provSourceSchema(joinPoint, ID_MSG, sourceSchemaVersionID, catalogName, operatorType);
		provTargetSchema(joinPoint, ID_MSG, targetSchemaVersionID, catalogName, operatorType);
		provComplexTable(joinPoint, ID_MSG, "sourceTable", sourceTableRef.getRefId(), sourceTableName, operatorType);
		provComplexTable(joinPoint, ID_MSG, "targetTable", targetTable1Ref.getRefId(), targetTable1Name, operatorType);
		provComplexTable(joinPoint, ID_MSG, "targetTable", targetTable2Ref.getRefId(), targetTable2Name, operatorType);
		// Target table's columns
		Column collTarget = null;
		Column collSource = null;
		for (Object col : columns1) {
			collTarget = (Column) col;
			provInputValue(joinPoint, ID_MSG, "targetColumnName", "inColumnName", collTarget.getName(), operatorType);
		}

		// Target columns' values
		provComplexColumnValues(joinPoint, ID_MSG, "targetColumn", "targetColumnValue", valoresDMODecompose1, catalog,
				operatorType);
		provComplexColumnValues(joinPoint, ID_MSG, "targetColumn", "targetColumnValue", valoresDMODecompose2, catalog,
				operatorType);

		// Previous tables
		ImmutableSet<Table> tables = catalog.getTables();
		for (Table t : tables) {
			previousName = t.getName();
			if (!previousName.equalsIgnoreCase(targetTable1Ref.getRefId())
					&& !previousName.equalsIgnoreCase(targetTable2Ref.getRefId())
					&& !previousName.equalsIgnoreCase(sourceTableRef.getRefId()))
				provSimpleTable(joinPoint, ID_MSG, "previousTable",
						refLog.getTableRef(targetVersion.getParent(), previousName).getRefId(), previousName,
						operatorType);
		}
		final String nested_drop_ID_MSG = ID_MSG + "Drop";
		HashMap<Object, Object> valoresDMO = new HashMap<Object, Object>();
		dropTableName = sourceTableName;
		dropTable1Name = sourceTableName;
		valoresDMO.put(dropTableName, catalog.getTable(sourceTableRef.getRefId()));
		obtenRowIDAndDMOsql(0, true);
		valoresDMO.put(targetTable1Ref, targetTable1Name);
		valoresDMO.put(targetTable2Ref, targetTable2Name);
		dropToProv(joinPoint, nested_drop_ID_MSG, catalog, refLog, version2.getChild(), sourceSchemaVersionID,
				targetSchemaVersionID, sourceTableRef, valoresDMO);
		provSuccessEndOperation(joinPoint, ID_MSG, operation, operatorType, start);
		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID, operatorType, ID_MSG);
		try {
			bgmm.fireEvent("operationEnd", initMethod);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		bgmm.removeListener(lis);

	}

	public void mergeToProv(JoinPoint joinPoint, String ID_MSG, Catalog catalog, RefLog refLog, Version targetVersion,
			String sourceSchemaVersionID, String targetSchemaVersionID, TableRef sourceTable1Ref,
			TableRef sourceTable2Ref, TableRef targetTableRef, HashMap<Object, Object> valoresDMO) {

		previousMerge = true;
		String operation = auxOperation(joinPoint);
		String operatorType = "MergeTable";
		String catalogName = catalog.getName();
		String idColumnSource1 = null, idColumnSource2 = null, idColumnTarget = null, previousName = null;
		String sourceTable1Name = sourceTable1Ref.getName();
		String sourceTable2Name = sourceTable2Ref.getName();
		String targetTableName = targetTableRef.getName();
		Table targetTable = catalog.getTable(targetTableRef.getRefId());
		ImmutableList<Column> columns = targetTable.getColumns();
		UUID idColumnUUID = null;

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

		ID_MSG = ID_MSG.replaceAll("Create", "Merge");
		BigInteger b = new BigInteger(EXECUTION_ID);
		BigInteger b2 = b.add(new BigInteger("1"));
		String EXECUTION_ID_CREATE = String.valueOf(b2);
		ID_MSG = ID_MSG.replaceAll(EXECUTION_ID_CREATE, EXECUTION_ID);
		SetBindingsListener lis = initiateListener(joinPoint, ID_MSG, operatorType);
		provInformedByOperationMergeDecompose(joinPoint, ID_MSG, "Merge");
		provAuthor(joinPoint, ID_MSG, author, operatorType);
		provInputValue(joinPoint, ID_MSG, "targetTableName", "inTableName", targetTableName, operatorType);
		provSourceSchema(joinPoint, ID_MSG, sourceSchemaVersionID, catalogName, operatorType);
		provTargetSchema(joinPoint, ID_MSG, targetSchemaVersionID, catalogName, operatorType);
		provComplexTable(joinPoint, ID_MSG, "sourceTable", sourceTable1Ref.getRefId(), sourceTable1Name, operatorType);
		provComplexTable(joinPoint, ID_MSG, "sourceTable", sourceTable2Ref.getRefId(), sourceTable2Name, operatorType);
		provComplexTable(joinPoint, ID_MSG, "targetTable", targetTableRef.getRefId(), targetTableName, operatorType);

		// Target columns' values
		provComplexColumnValues(joinPoint, ID_MSG, "targetColumn", "targetColumnValue", valoresDMO, catalog,
				operatorType);
		// Previous tables
		ImmutableSet<Table> tables = catalog.getTables();
		for (Table t : tables) {
			previousName = t.getName();
			if (!previousName.equalsIgnoreCase(targetTableRef.getRefId())
					&& !previousName.equalsIgnoreCase(sourceTable1Ref.getRefId())
					&& !previousName.equalsIgnoreCase(sourceTable2Ref.getRefId()))
				provSimpleTable(joinPoint, ID_MSG, "previousTable",
						refLog.getTableRef(targetVersion.getParent(), previousName).getRefId(), previousName,
						operatorType);
		}

		String nested_drop_ID_MSG = ID_MSG + "Drop1";
		valoresDMO = new HashMap<Object, Object>();
		dropTable1Name = sourceTable1Name;
		dropTableName = sourceTable1Name;
		valoresDMO.put(dropTable1Name, catalog.getTable(sourceTable1Ref.getRefId()));
		obtenRowIDAndDMOsql(0, true);
		valoresDMO.put(targetTableRef, targetTableName);
		dropToProv(joinPoint, nested_drop_ID_MSG, catalog, refLog, targetVersion.getChild(), sourceSchemaVersionID,
				targetSchemaVersionID, sourceTable1Ref, valoresDMO);

		nested_drop_ID_MSG = ID_MSG + "Drop2";
		valoresDMO = new HashMap<Object, Object>();
		dropTable2Name = sourceTable2Name;
		dropTableName = sourceTable2Name;
		valoresDMO.put(dropTable2Name, catalog.getTable(sourceTable2Ref.getRefId()));
		obtenRowIDAndDMOsql(0, true);
		valoresDMO.put(targetTableRef, targetTableName);
		dropToProv(joinPoint, nested_drop_ID_MSG, catalog, refLog, targetVersion.getChild(), sourceSchemaVersionID,
				targetSchemaVersionID, sourceTable2Ref, valoresDMO);
		provSuccessEndOperation(joinPoint, ID_MSG, operation, operatorType, start);
		BGMEvent initMethod = new BGMEvent(joinPoint.getTarget(), EXECUTION_ID, operatorType, ID_MSG);
		try {
			bgmm.fireEvent("operationEnd", initMethod);
		} catch (InvocationTargetException e) {
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
		} else
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
		String sourceTableName = null, sourceTable2Name = null;
		HashMap<Object, Object> resultado = new HashMap<Object, Object>();
		HashMap<Object, Object> resultadoAux = new HashMap<Object, Object>();
		Table targetTable, targetTable2;
		TableRef targetTableRef = null;
		String targetTableName = null;// ,targetTable2Name = null;
		ImmutableList<Column> targetColumns = null;
		if (superOperationType != null && superOperationType.equalsIgnoreCase("DecomposeTable")) {
			if (targetTable2Ref == null)
				targetTableRef = targetTable1Ref;
			else
				targetTableRef = targetTable2Ref;
		}
		if (DMOsql != null) {
			int indexSelect = DMOsql.indexOf("select");
			int indexUnion = DMOsql.indexOf("union");
			////////////////////////////////////////////////////////////////////////////
			// Is insert select
			////////////////////////////////////////////////////////////////////////////
			if (indexSelect != -1) {
				insertSelect = true;
				String[] palabras = DMOsql.split("\\s+");
				// No union
				if (indexUnion == -1) {
					if (palabras[4].equals("*")) {
						targetTableName = palabras[2];
						sourceTableName = palabras[6].substring(0, palabras[6].length() - 1);
					} else {
						sourceTableName = sourceTableNameDecom;
						targetTableName = targetTableRef.getName();
					}
				} else {
					insertSelectUnion = true;
					targetTableName = palabras[2];
					sourceTableName = palabras[6].substring(0, palabras[6].length());
					sourceTable2Name = palabras[11].substring(0, palabras[11].length() - 2);
				}
				try {
					targetTableRef = refLog.getTableRef(targetSchemaVersion, targetTableName);
				} catch (IllegalArgumentException e) {
					targetTableRef = refLog.getTableRef(sourceSchemaVersion, targetTableName);
				}
				targetTable = catalog.getTable(targetTableRef.getRefId());
				targetColumns = targetTable.getColumns();
				resultado.put("targetTable", targetTableRef);
				TableRef sourceTableRef = null, sourceTable2Ref = null;
				try {
					sourceTableRef = refLog.getTableRef(targetSchemaVersion, sourceTableName);
				} catch (IllegalArgumentException e) {
					sourceTableRef = refLog.getTableRef(sourceSchemaVersion, sourceTableName);
				}
				Table sourceTable = catalog.getTable(sourceTableRef.getRefId());
				resultado.put("sourceTable", sourceTableRef);
				List<Column> aux = new ArrayList<Column>();
				// select * (union or not union- merge or copy)
				if (palabras[4].equals("*")) {
					ImmutableList<Column> sourceColumns = sourceTable.getColumns();
					for (i = 0; i < targetColumns.size(); i++) {
						aux.add(sourceColumns.get(i));
						resultado.put(targetColumns.get(i), aux);
					}
				} // not union and select with fields --> decompose
				else if (indexUnion == -1 && !palabras[4].equals("*")) {
					List<String> stringSourceColumns = new ArrayList<String>();
					int j = 4;
					int aux2 = 0;
					while (j < palabras.length - 2) {
						aux2 = palabras[j].indexOf(',');
						if (aux2 != -1) {
							stringSourceColumns.add(palabras[j].substring(0, aux2));
						} else {
							stringSourceColumns.add(palabras[j]);
						}
						j++;
					}
					ImmutableList<Column> sourceColumns = sourceTable.getColumns();
					List<Column> aux3 = new ArrayList<Column>();
					for (j = 0; j < targetColumns.size(); j++) {
						aux3 = new ArrayList<Column>();
						for (i = 0; i < sourceColumns.size(); i++) {
							if (targetColumns.get(j).getName().equalsIgnoreCase(sourceColumns.get(i).getName())) {
								aux3.add(sourceColumns.get(i));
								resultado.put(targetColumns.get(j), aux3);
								resultadoAux.put(targetColumns.get(j), aux3);
								break;
							}
						}
					}
					if (columnasTargetSource1 == null)
						columnasTargetSource1 = resultadoAux;
					else
						columnasTargetSource2 = resultadoAux;

				}
				if (insertSelectUnion) {
					try {
						sourceTable2Ref = refLog.getTableRef(targetSchemaVersion, sourceTable2Name);
					} catch (IllegalArgumentException e) {
						sourceTable2Ref = refLog.getTableRef(sourceSchemaVersion, sourceTable2Name);
					}
					Table sourceTable2 = catalog.getTable(sourceTable2Ref.getRefId());
					resultado.put("sourceTableSecondTable", sourceTable2Ref);
					ImmutableList<Column> sourceColumns2 = sourceTable2.getColumns();
					for (i = 0; i < targetColumns.size(); i++) {
						aux = (List<Column>) resultado.get(targetColumns.get(i));
						aux.add(sourceColumns2.get(i));
						resultado.put(targetColumns.get(i), aux);
					}
				}
				////////////////////////////////////////////////////////////////////////////
				// normal insert
				////////////////////////////////////////////////////////////////////////////
			} else {
				String aux = DMOsql.substring(DMOsql.indexOf("INTO")).trim();
				String aux2 = aux.substring(4).trim();
				int indexBlank = aux2.indexOf(" ");
				targetTableName = aux2.substring(0, indexBlank);
				int indexFirstLeftBracket = DMOsql.indexOf("(");
				int indexFirstRightBracket = DMOsql.indexOf(")");
				int indexSecondLeftBracket = DMOsql.lastIndexOf("(");
				int indexSecondRightBracket = DMOsql.lastIndexOf(")");
				// insert in all columns
				if (indexFirstLeftBracket == indexSecondLeftBracket) {
					String bracket = DMOsql.substring(indexFirstLeftBracket + 1, indexFirstRightBracket);
					String[] arrayValues = bracket.split(",");
					targetTableRef = refLog.getTableRef(targetSchemaVersion, targetTableName);
					targetTable = catalog.getTable(targetTableRef.getRefId());
					resultado.put("targetTable", targetTableRef);
					resultado.put("type", "allColumns");
					ImmutableList<Column> columns = targetTable.getColumns();
					i = 0;
					while (i < arrayValues.length) {
						resultado.put(columns.get(i), arrayValues[i]);
						i++;
					}
				}
				// insert of concrete columns
				else {
					String firstBracket = DMOsql.substring(indexFirstLeftBracket + 1, indexFirstRightBracket);
					String secondBracket = DMOsql.substring(indexSecondLeftBracket + 1, indexSecondRightBracket);
					String[] arrayColumns = firstBracket.split(",");
					String[] arrayValues = secondBracket.split(",");
					targetTableRef = refLog.getTableRef(targetSchemaVersion, targetTableName);
					targetTable = catalog.getTable(targetTableRef.getRefId());
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
			if (superOperationType != null && superOperationType.equalsIgnoreCase("MergeTable")) {
				aux2 = "MergeTable";
			} else if (superOperationType != null && superOperationType.equalsIgnoreCase("DecomposeTable")) {
				aux2 = "DecomposeTable";
			} else {
				fullClassName = joinPoint.getTarget().getClass().getName();
				className = fullClassName.substring(fullClassName.lastIndexOf(".") + 1);
				aux = className.lastIndexOf("Migrator");
				aux2 = className.substring(0, aux);
			}
			return aux2;
		} else {
			return DMOsql;
		}
	}

	private String auxColumnValueID(Object primaryKey, int schema, String tableName, String columnName) {
		return "IdColumnValue_" + primaryKey + "_" + schema + "_" + tableName + "_" + columnName;
	}

	private String auxColumnID(int schema, String tableName, String columnName) {
		return "IdColumn_" + schema + "_" + tableName + "_" + columnName;
	}

	private String auxRowID(ArrayList<String> keyValues) {
		String id = "";
		boolean enter = false;
		for (int i = 0; i < keyValues.size(); i++) {
			if (enter)
				id = id + "_";
			enter = true;
			id = id + keyValues.get(i);
		}
		return id;
	}

	public String auxOperationType() {
		if (schemaOperation)
			return "smo";
		else
			return "dmo";
	}

	private void provInformedByOperationMergeDecompose(JoinPoint joinPoint, String ID_MSG, String operation) {
		Identified target = null;
		target = (Identified) joinPoint.getTarget();
		ID_MSG = ID_MSG.replaceAll("Create", operation);
		BigInteger b = new BigInteger(EXECUTION_ID);
		BigInteger b2 = b.add(new BigInteger("1"));
		String EXECUTION_ID_CREATE = String.valueOf(b2);
		ID_MSG = ID_MSG.replaceAll(EXECUTION_ID_CREATE, EXECUTION_ID);
		String aux = ID_MSG;
		int i = aux.lastIndexOf(operation);
		aux = aux.substring(0, i);
		if (superOperationType.equalsIgnoreCase("DecomposeTable")) {
			provenance(EXECUTION_ID, target, ID_MSG, "smoCT", ID_MSG + "Create1", "CreateTable");
			provenance(EXECUTION_ID, target, ID_MSG, "smoCT", ID_MSG + "Create2", "CreateTable");
			provenance(EXECUTION_ID, target, ID_MSG, "dmoI", ID_MSG + "Insert1", "Insert");
			provenance(EXECUTION_ID, target, ID_MSG, "dmoI", ID_MSG + "Insert2", "Insert");
		} else
			provenance(EXECUTION_ID, target, ID_MSG, "smoCT", ID_MSG + "Create", "CreateTable");
		if (superOperationType.equalsIgnoreCase("MergeTable")) {
			provenance(EXECUTION_ID, target, ID_MSG, "smoDT", ID_MSG + "Drop1", "DropTable");
			provenance(EXECUTION_ID, target, ID_MSG, "smoDT", ID_MSG + "Drop2", "DropTable");
			provenance(EXECUTION_ID, target, ID_MSG, "dmoI", ID_MSG + "Insert1", "Insert");
		} else {
			provenance(EXECUTION_ID, target, ID_MSG, "smoDT", ID_MSG + "Drop", "DropTable");
		}
	}

	private void provInformedByOperationCopy(JoinPoint joinPoint, String ID_MSG) {
		Identified target = null;
		target = (Identified) joinPoint.getTarget();
		provenance(EXECUTION_ID, target, ID_MSG, "smoCT", ID_MSG + "Create", "CreateTable");
		provenance(EXECUTION_ID, target, ID_MSG, "dmoI", ID_MSG + "Insert1", "Insert");
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
		if (operatorType.equalsIgnoreCase("MergeTable")) {
			ID_MSG = ID_MSG.replaceAll("Create", "Merge");
			BigInteger b = new BigInteger(EXECUTION_ID);
			BigInteger b2 = b.add(new BigInteger("1"));
			String EXECUTION_ID_CREATE = String.valueOf(b2);
			ID_MSG = ID_MSG.replaceAll(EXECUTION_ID_CREATE, EXECUTION_ID);
		}
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
		if (operatorType.equalsIgnoreCase("MergeTable")) {
			ID_MSG = ID_MSG.replaceAll("Create", "Merge");
			BigInteger b = new BigInteger(EXECUTION_ID);
			BigInteger b2 = b.add(new BigInteger("1"));
			String EXECUTION_ID_CREATE = String.valueOf(b2);
			ID_MSG = ID_MSG.replaceAll(EXECUTION_ID_CREATE, EXECUTION_ID);
		}
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
		provenanceValueType(target, "EndTransTime", EXECUTION_ID, ID_MSG, completeID, "endTransTime",
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
		provenanceValueType(target, "tSchemaName", EXECUTION_ID, ID_MSG, completeID, "schemaName", catalogName,
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
		} else if (varName.equalsIgnoreCase("sourceTable")) {
			prefix1 = "st";
			prefix2 = "s";
		}
		if (operatorType.equalsIgnoreCase("DropTable") || operatorType.equalsIgnoreCase("MergeTable")
				|| operatorType.equalsIgnoreCase("DecomposeTable")) {
			provenanceValueType(target, prefix1 + "EndTransTime", EXECUTION_ID, ID_MSG, completeID, "endTransTime",
					String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
		} else {
			provenanceValueType(target, prefix1 + "StartTransTime", EXECUTION_ID, ID_MSG, completeID, "startTransTime",
					String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
		}
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
		String completeID = IdentifiersManager.getIdentifier(target).getCompleteUUID() + "_" + value;
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
		} else if (nameVar.equalsIgnoreCase("sourceColumn")) {
			prefix1 = "sc";
			prefix2 = "s";
		}
		provenanceValueType(target, prefix1 + "StartTransTime", EXECUTION_ID, ID_MSG, completeID, "startTransTime",
				String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
		provenanceValueType(target, prefix2 + "ColumnType", EXECUTION_ID, ID_MSG, completeID, "typeName", columnType,
				operatorType);

		provenanceValueType(target, prefix2 + "ColumnName", EXECUTION_ID, ID_MSG, completeID, "columnName", columnName,
				operatorType);
		/////// In copy, create, decompose
		if (nameVar.equals("targetColumn")) {
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
		if (varName.equalsIgnoreCase("sourceColumn")) {
			prefix = "s";
		} else if (varName.equalsIgnoreCase("inputColumnDef")) {
			prefix = "in";
		}

		if (operatorType.equalsIgnoreCase("DropTable") || operatorType.equalsIgnoreCase("MergeTable")
				|| operatorType.equalsIgnoreCase("DecomposeTable")) {
			provenanceValueType(target, prefix + "cEndTransTime", EXECUTION_ID, ID_MSG, completeID, "endTransTime",
					String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
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
		if (operatorType.equalsIgnoreCase("DropTable") || operatorType.equalsIgnoreCase("MergeTable")
				|| operatorType.equalsIgnoreCase("DecomposeTable")) {
			provenanceValueType(target, prefix + "cvEndTransTime", EXECUTION_ID, ID_MSG, completeID, "endTransTime",
					String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
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
		PreparedStatement ps = null, ps2 = null;
		ResultSetMetaData rsM = null, rsM2 = null;
		ResultSet rs = null, rs2 = null;
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
		ResultSet pkColumns = null;
		String primaryKeyName = null;
		int primaryKeyOrder = 0;
		Object primaryKeyValue = null;
		String targetColumnValueId, sourceColumnValueId = null;
		String columnValue = null, rowId = null;
		Table sourceTable = null, sourceTable2 = null;
		Table targetTable = null;
		TableRef sourceTableRef = null, sourceTable2Ref = null;
		TableRef targetTableRef = null;
		UUID idTargetColumnUUID, idSourceColumnUUID = null;
		String idTargetColumn, idSourceColumn = null;
		String sourceColumnName = null, targetColumnName = null;
		Column targetColumn = null, sourceColumn = null;
		Set<Object> columnasInsert = null;
		List<Object> columnasInsertList = new ArrayList<Object>();
		ArrayList<String> keyValues = new ArrayList<String>();
		ImmutableList<Column> keyColumns = null, keyColumns2 = null;
		int i = 1, numColumns = 0, numColumns2 = 0;
		int numKeyColumns = 0, numKeyColumns2 = 0;
		String value = null;
		ArrayList<String> keyColumnsSelectList = new ArrayList<String>();
		ArrayList<String> keyColumnsSelectList2 = new ArrayList<String>();
		String keyColumnsSelect = "", keyColumnsSelect2 = "";
		boolean entra = false, entra2 = false;

		if (insertSelect && !operatorType.equalsIgnoreCase("DropTable")) {
			try {
				con = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
				targetTableRef = (TableRef) valoresDMOmethod.get("targetTable");
				sourceTableRef = (TableRef) valoresDMOmethod.get("sourceTable");
				targetTable = catalog.getTable(targetTableRef.getRefId());
				sourceTable = catalog.getTable(sourceTableRef.getRefId());
				valoresDMOmethod.remove("targetTable");
				valoresDMOmethod.remove("sourceTable");
				if (insertSelectUnion) {
					sourceTable2Ref = (TableRef) valoresDMOmethod.get("sourceTableSecondTable");
					sourceTable2 = catalog.getTable(sourceTable2Ref.getRefId());
					valoresDMOmethod.remove("sourceTableSecondTable");
				}
				pkColumns = con.getMetaData().getPrimaryKeys(null, null, sourceTable.getName());
				primaryKeyName = null;
				while (pkColumns.next()) {
					primaryKeyName = pkColumns.getString("COLUMN_NAME");
				}
				pkColumns.close();
				if (superOperationType != null && superOperationType.equalsIgnoreCase("DecomposeTable")) {
					Set<Object> listaCamposTarget = (Set<Object>) valoresDMOmethod.keySet();
					Column sourceC = null;
					int ii = 0;
					Column cc = null;
					for (Object c : listaCamposTarget) {
						ii++;
						cc = (Column) c;
						sourceC = (Column) ((List<Column>) valoresDMOmethod.get(cc)).get(0);
						numKeyColumns++;
						if (!entra) {
							keyColumnsSelectList.add(sourceC.getName());
							keyColumnsSelect = sourceC.getName();
							if (sourceC.getName().equalsIgnoreCase(primaryKeyName)) {
								primaryKeyOrder = ii;
							}
							entra = true;

						} else {
							if (sourceC.getName().equalsIgnoreCase(primaryKeyName)) {
								primaryKeyOrder = ii;
							}
							keyColumnsSelectList.add(sourceC.getName());
							keyColumnsSelect = keyColumnsSelect + "," + sourceC.getName();
						}
					}
				} else { // copy y el merge
					keyColumns = sourceTable.getColumns();
					int ii = 0;
					for (Column c : keyColumns) {
						ii++;
						numKeyColumns++;
						if (!entra) {
							keyColumnsSelectList.add(c.getName());
							keyColumnsSelect = c.getName();
							if (c.getName().equalsIgnoreCase(primaryKeyName)) {
								primaryKeyOrder = ii;
							}
							entra = true;

						} else {
							if (c.getName().equalsIgnoreCase(primaryKeyName)) {
								primaryKeyOrder = ii;
							}
							keyColumnsSelectList.add(c.getName());
							keyColumnsSelect = keyColumnsSelect + "," + c.getName();
						}
					}
					// Merge
					if (insertSelectUnion) {
						keyColumns2 = sourceTable2.getColumns();
						ii = 0;
						for (Column c : keyColumns2) {
							ii++;
							numKeyColumns2++;
							if (!entra2) {
								keyColumnsSelectList2.add(c.getName());
								keyColumnsSelect2 = c.getName();
								if (c.getName().equalsIgnoreCase(primaryKeyName)) {
									primaryKeyOrder = ii;
								}
								entra2 = true;

							} else {
								if (c.getName().equalsIgnoreCase(primaryKeyName)) {
									primaryKeyOrder = ii;
								}
								keyColumnsSelectList2.add(c.getName());
								keyColumnsSelect2 = keyColumnsSelect2 + "," + c.getName();
							}
						}
					}
				}
				ps = con.prepareStatement("select " + keyColumnsSelect + " from " + sourceTable.getName()
						+ " order by row_number() over() desc limit " + numberSelectCount);

				if (insertSelectUnion) {
					ps2 = con.prepareStatement("select " + keyColumnsSelect2 + " from " + sourceTable2.getName()
							+ " order by row_number() over() desc limit " + numberSelectCountUnionSecondTable);
				}

				columnasTarget = (Set<Object>) valoresDMOmethod.keySet();
				columnasTargetList.addAll(columnasTarget);

				//// UNION/////////////////////////////////////////////////////7
				targetColumn = null;
				sourceColumn = null;
				int contadorFilas = 0;
				rs = ps.executeQuery();
				rsM = ps.getMetaData();
				numColumns = rsM.getColumnCount();
				while (rs.next()) {
					contadorFilas = 1;
					keyValues.clear();
					for (int j = 1; j <= numKeyColumns; j++) {
						keyValues.add(rs.getObject(j).toString());
					}
					i = 1;
					boolean PKObtained = false;
					while (i <= numColumns) {
						columnValue = rs.getObject(i).toString();
						if (!PKObtained) {
							primaryKeyValue = rs.getObject(primaryKeyOrder).toString();
						}
						targetColumnName = keyColumnsSelectList.get(i - 1);
						for (Object tc : columnasTarget) {
							if (((Column) tc).getName().equalsIgnoreCase(targetColumnName)) {
								targetColumn = (Column) tc;
							}
						}
						idTargetColumn = auxColumnID(targetSchemaNumber, targetTableRef.getName(),
								targetColumn.getName());
						for (Object o : columnasTarget) {
							Column aux = (Column) o;
							Column sourceC = (Column) ((List<Column>) valoresDMOmethod.get(targetColumn)).get(0);
							if (aux.getName().equalsIgnoreCase(targetColumnName)) {
								List<Column> sourceColumns = (List<Column>) valoresDMOmethod.get(targetColumn);
								for (Column oso : sourceColumns) {
									if (targetColumnName.equalsIgnoreCase(oso.getName())) {
										sourceColumn = oso;
									}
								}
							}

						}
						sourceColumnName = sourceColumn.getName();
						idSourceColumn = auxColumnID(targetSchemaNumber - 1, sourceTable.getName(),
								sourceColumn.getName());
						columnValue = columnValue.replace("'", "");
						if (contadorFilas == 1) {
							if (columnValueVar.equalsIgnoreCase("newColumnValue")) {
								provSimpleColumn(joinPoint, ID_MSG, targetColumnName, columnVar, null, idTargetColumn,
										operatorType);
							} else if (columnValueVar.equalsIgnoreCase("targetColumnValue")) {
								ImmutableList<Column> columns = targetTable.getColumns();
								for (Column col : columns) {
									if (col.getName().equalsIgnoreCase(targetColumn.getName())) {
										provSimpleColumn(joinPoint, ID_MSG, col.getName(), "sourceColumn", null,
												idSourceColumn, operatorType);
										provComplexColumn(joinPoint, ID_MSG, col.getName(), col.getType().toString(),
												"targetColumn", idTargetColumn, "sourceColumn", idSourceColumn,
												operatorType);
									}
								}
							}
						}
						sourceColumnValueId = auxColumnValueID(primaryKeyValue, (targetSchemaNumber - 1),
								sourceTable.getName(), sourceColumnName);
						if (columnValueVar.equalsIgnoreCase("newColumnValue")) {
							provSimpleColumnValue(joinPoint, ID_MSG, "value", sourceColumnName, columnValue,
									sourceColumnValueId, auxRowID(keyValues), operatorType);
						} else if (columnValueVar.equalsIgnoreCase("targetColumnValue")) {
							provSimpleColumnValue(joinPoint, ID_MSG, "sourceColumnValue", sourceColumnName, columnValue,
									sourceColumnValueId, auxRowID(keyValues), operatorType);
						}
						targetColumnValueId = auxColumnValueID(primaryKeyValue, targetSchemaNumber,
								targetTable.getName(), targetColumnName);
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
						/////// insert
						if (columnValueVar.equals("newColumnValue")) {
							provenanceValueType(target, "value", EXECUTION_ID, ID_MSG, completeID, "linked",
									sourceColumnValueId, operatorType);
							provenanceValueType(target, "column", EXECUTION_ID, ID_MSG, completeID, "linked",
									idTargetColumn, operatorType);
						}
						////// Copy/merge/decompose
						else if (columnValueVar.equals("targetColumnValue")) {
							provenanceValueType(target, "sourceColumnValue", EXECUTION_ID, ID_MSG, completeID, "linked",
									sourceColumnValueId, operatorType);
							provenanceValueType(target, "targetColumn", EXECUTION_ID, ID_MSG, completeID, "linked",
									idTargetColumn, operatorType);
						}
						i++;
					}
				}
				rs.close();
				ps.close();
				////////////////////////////////////////////////
				if (insertSelectUnion) {
					targetColumn = null;
					sourceColumn = null;
					contadorFilas = 0;
					rs2 = ps2.executeQuery();
					rsM2 = ps2.getMetaData();
					numColumns = rsM2.getColumnCount();
					while (rs2.next()) {
						contadorFilas = 1;
						keyValues.clear();
						for (int j = 1; j <= numKeyColumns2; j++) {
							keyValues.add(rs2.getObject(j).toString());
						}
						i = 1;
						boolean PKObtained = false;
						while (i <= numColumns) {
							columnValue = rs2.getObject(i).toString();
							if (!PKObtained) {
								primaryKeyValue = rs.getObject(primaryKeyOrder).toString();
							}
							targetColumnName = keyColumnsSelectList.get(i - 1);
							for (Object tc : columnasTarget) {
								if (((Column) tc).getName().equalsIgnoreCase(targetColumnName)) {
									targetColumn = (Column) tc;
								}
							}
							/////////////////////////////// Associated Columns in target
							idTargetColumn = auxColumnID(targetSchemaNumber, targetTableRef.getName(),
									targetColumn.getName());
							////////////////////////////// Source Values in source
							for (Object o : columnasTarget) {
								Column aux = (Column) o;
								Column sourceC = (Column) ((List<Column>) valoresDMOmethod.get(targetColumn)).get(0);
								if (aux.getName().equalsIgnoreCase(targetColumnName)) {
									List<Column> sourceColumns = (List<Column>) valoresDMOmethod.get(targetColumn);
									for (Column oso : sourceColumns) {
										if (targetColumnName.equalsIgnoreCase(oso.getName())) {
											sourceColumn = oso;
										}
									}
								}

							}
							sourceColumnName = sourceColumn.getName();
							idSourceColumn = auxColumnID(targetSchemaNumber - 1, sourceTable2.getName(),
									sourceColumn.getName());
							columnValue = columnValue.replace("'", "");
							if (contadorFilas == 1) {
								if (columnValueVar.equalsIgnoreCase("newColumnValue")) {
									provSimpleColumn(joinPoint, ID_MSG, targetColumnName, columnVar, null,
											idTargetColumn, operatorType);
								} else if (operatorType.equalsIgnoreCase("MergeTable")) {
									ImmutableList<Column> columns = targetTable.getColumns();
									for (Column col : columns) {
										provComplexColumn(joinPoint, ID_MSG, col.getName(), col.getType().toString(),
												"targetColumn", idTargetColumn, "sourceColumn", idSourceColumn,
												operatorType);
									}
								}
							}
							sourceColumnValueId = auxColumnValueID(primaryKeyValue, (targetSchemaNumber - 1),
									sourceTable2.getName(), sourceColumnName);
							if (columnValueVar.equalsIgnoreCase("newColumnValue")) {
								provSimpleColumnValue(joinPoint, ID_MSG, "value", sourceColumnName, columnValue,
										sourceColumnValueId, auxRowID(keyValues), operatorType);
							} else if (columnValueVar.equalsIgnoreCase("targetColumnValue")) {
								provSimpleColumnValue(joinPoint, ID_MSG, "sourceColumnValue", sourceColumnName,
										columnValue, sourceColumnValueId, auxRowID(keyValues), operatorType);
							}
							targetColumnValueId = auxColumnValueID(primaryKeyValue, targetSchemaNumber,
									targetTable.getName(), targetColumnName);
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
							/////// insert
							if (columnValueVar.equals("newColumnValue")) {
								provenanceValueType(target, "value", EXECUTION_ID, ID_MSG, completeID, "linked",
										sourceColumnValueId, operatorType);
								provenanceValueType(target, "column", EXECUTION_ID, ID_MSG, completeID, "linked",
										idTargetColumn, operatorType);
							}
							////// merge/copy/decompose
							else if (columnValueVar.equals("targetColumnValue")) {
								provenanceValueType(target, "sourceColumnValue", EXECUTION_ID, ID_MSG, completeID,
										"linked", sourceColumnValueId, operatorType);
								provenanceValueType(target, "targetColumn", EXECUTION_ID, ID_MSG, completeID, "linked",
										idTargetColumn, operatorType);
							}
							i++;
						}
					}
					rs2.close();
					ps2.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else if (operatorType.equalsIgnoreCase("DropTable")) {
			try {
				con = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
				sourceTable = (Table) valoresDMOmethod.get(dropTableName);
				ImmutableList<Column> columns = sourceTable.getColumns();
				ps = con.prepareStatement("select * from " + sourceTable.getName()
						+ " order by row_number() over() desc limit " + lastRowId);
				rs = ps.executeQuery();
				rsM = ps.getMetaData();
				numColumns = rsM.getColumnCount();
				//// UNION/////////////////////////////////////////////////////7
				sourceColumn = null;
				int contadorFilas = 0;
				while (rs.next()) {
					contadorFilas = 1;
					keyValues.clear();
					for (int j = 1; j <= numColumns; j++) {
						keyValues.add(rs.getObject(j).toString());
					}
					i = 1;
					while (i <= numColumns) {
						columnValue = rs.getObject(i).toString();
						if (i == 1) {
							primaryKeyValue = columnValue;
						}
						sourceColumn = (Column) columns.get(i - 1);
						sourceColumnName = sourceColumn.getName();
						idSourceColumn = auxColumnID(targetSchemaNumber - 1, sourceTable.getName(),
								sourceColumn.getName());
						columnValue = columnValue.replace("'", "");
						sourceColumnValueId = auxColumnValueID(primaryKeyValue, (targetSchemaNumber - 1),
								sourceTable.getName(), sourceColumnName);
						provenance(EXECUTION_ID, target, ID_MSG, columnValueVar, sourceColumnValueId, operatorType);
						provenanceValueType(target, "scvEndTransTime", EXECUTION_ID, ID_MSG, completeID, "endTransTime",
								String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
						provenanceValueType(target, "columnValue", EXECUTION_ID, ID_MSG, completeID, "columnValue",
								columnValue, operatorType);
						provenanceValueType(target, "rowId", EXECUTION_ID, ID_MSG, completeID, "rowID",
								auxRowID(keyValues), operatorType);
						i++;
					}
				}
				rs.close();
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
		// Usual insert
		else {
			targetTableRef = (TableRef) valoresDMOmethod.get("targetTable");
			targetTable = catalog.getTable(targetTableRef.getRefId());
			ArrayList<Column> keyColumns3 = (ArrayList<Column>) targetTable.getPrimaryKeyColumns();
			valoresDMOmethod.remove("type");
			valoresDMOmethod.remove("targetTable");
			columnasInsert = (Set<Object>) valoresDMOmethod.keySet();
			columnasInsertList.addAll(columnasInsert);
			for (Column c : keyColumns3) {
				keyValues.add(valoresDMOmethod.get(c).toString().replace("'", ""));
			}
			rowId = auxRowID(keyValues);
			i = 1;
			numColumns = columnasInsert.size();
			primaryKeyValue = null;
			boolean primaryKeyB = false;
			while (i <= numColumns) {
				if (primaryKeyB) {
					primaryKeyB = false;
					primaryKeyValue = columnValue;
				}
				targetColumn = (Column) columnasInsertList.get(i - 1);
				targetColumnName = targetColumn.getName();
				columnValue = (String) valoresDMOmethod.get(targetColumn);
				idTargetColumn = auxColumnID(targetSchemaNumber, targetTableRef.getName(), targetColumnName);
				provSimpleColumn(joinPoint, ID_MSG, targetColumnName, columnVar, null, idTargetColumn, operatorType);
				provenance(EXECUTION_ID, target, ID_MSG, columnValueVar,
						auxColumnValueID(primaryKeyValue, targetSchemaNumber, targetTable.getName(), targetColumnName),
						operatorType);
				provenanceValueType(target, "StartTransTime", EXECUTION_ID, ID_MSG, completeID, "startTransTime",
						String.valueOf(new Timestamp(System.currentTimeMillis())), operatorType);
				provenanceValueType(target, "columnValue", EXECUTION_ID, ID_MSG, completeID, "columnValue", columnValue,
						operatorType);
				provenanceValueType(target, "rowId", EXECUTION_ID, ID_MSG, completeID, "rowID", rowId, operatorType);
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
			e1.printStackTrace();
		}

	}

	private void provenanceValueType(final Identified target, String variableName, String ID_EXECUTION,
			String ID_EXECUTION_METHOD, String identifier, String type, String value, String operatorType) {
		try {
			BGMEvent e = new BGMEvent(target, ID_EXECUTION, operatorType, ID_EXECUTION_METHOD, variableName, value);
			bgmm.fireEvent("newValueBinding", e);
		} catch (InvocationTargetException e1) {
			e1.printStackTrace();
		}
	}

}
