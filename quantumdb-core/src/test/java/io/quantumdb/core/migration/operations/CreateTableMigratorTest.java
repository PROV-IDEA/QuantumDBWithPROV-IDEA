package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static org.junit.Assert.assertEquals;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.TableMapping;
import org.junit.Before;
import org.junit.Test;

public class CreateTableMigratorTest {

	private Catalog catalog;
	private Changelog changelog;
	private TableMapping tableMapping;
	private DataMappings dataMappings;
	private CreateTableMigrator migrator;

	@Before
	public void setUp() {
		this.catalog = new Catalog("test-db");
		this.changelog = new Changelog();
		this.tableMapping = TableMapping.bootstrap(changelog.getRoot(), catalog);
		this.dataMappings = new DataMappings(tableMapping, catalog);

		this.migrator = new CreateTableMigrator();
	}

	@Test
	public void testExpandForCopyingTable() {
		CreateTable operation = SchemaOperations.createTable("users")
				.with("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL)
				.with("name", varchar(255), NOT_NULL);

		changelog.addChangeSet("Michael de Jong", "Creating 'users' table.", operation);
		migrator.migrate(catalog, tableMapping, dataMappings, changelog.getLastAdded(), operation);

		String tableId = tableMapping.getTableId(changelog.getLastAdded(), "users");
		Table ghostTable = catalog.getTable(tableId);
		Table expectedGhostTable = new Table(tableId)
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("name", varchar(255), NOT_NULL));

		assertEquals(expectedGhostTable, ghostTable);
		assertEquals("users", tableMapping.getTableName(changelog.getLastAdded(), ghostTable.getName()));
	}

}
