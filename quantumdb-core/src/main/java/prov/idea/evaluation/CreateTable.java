package prov.idea.evaluation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class CreateTable {

	public static void main(String[] args) {
		Connection con;
		try {
			con = ConnectionUtils.createConnection();
			tearDownTable(con, "developer_sub_A");
			tearDownTable(con, "developer_sub_B");

			tearDownSequence(con, "even_seq");
			tearDownSequence(con, "odd_seq");
			createOddKeys(con);
			createEvenKeys(con);

			createTable(con, "developer_sub_A", "even_seq");
			createTable(con, "developer_sub_B", "odd_seq");
			fillTable(5, "developer_sub_A");
			fillTable(5, "developer_sub_B");

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	// Sequence to generate odd keys
	protected static void createOddKeys(Connection connection) throws SQLException {

		execute(connection,
				new StringBuilder().append("CREATE SEQUENCE odd_seq INCREMENT BY 2 START WITH 1;").toString());

	}

	// Sequence to generate even keys
	protected static void createEvenKeys(Connection connection) throws SQLException {

		execute(connection,
				new StringBuilder().append("CREATE SEQUENCE even_seq INCREMENT BY 2 START WITH 2;").toString());

	}

	protected static void createTable(Connection connection, String tableName, String sequenceName)
			throws SQLException {

		execute(connection,
				new StringBuilder().append("CREATE TABLE " + tableName + " (")
						.append("  id integer default nextval('" + sequenceName + "'), ")
						.append("  name varchar(64) NOT NULL, ").append("  surname varchar(64) NOT NULL, ")
						.append("  email varchar(255) NOT NULL ").append(");").toString());
	}

	protected static void fillTable(int tam, String tableName) {
		ExecutorService executorService = new ScheduledThreadPoolExecutor(4);
		Random random = new Random();

		try {
			AtomicInteger progressCounter = new AtomicInteger();
			executorService.submit(() -> {
				try (Connection connection = ConnectionUtils.createConnection()) {
					String query = "INSERT INTO " + tableName + " (name, surname, email)  VALUES (?,?, ?)";
					try (PreparedStatement statement = connection.prepareStatement(query)) {
						for (int j = 0; j < tam; j++) {// for (int j = 0; j < ; j++) {
							String name = UserUtils.pickName(random);
							String surname = UserUtils.pickSurname(random);
							String email = UserUtils.getEmail(name);

							statement.setString(1, name);
							statement.setString(2, surname);
							statement.setString(3, email);
							statement.addBatch();
						}
						statement.executeBatch();
					}
					int progress = progressCounter.incrementAndGet();
					if (progress % 50 == 0) {
						log.info("  Filling table with data: {}%", progress / 5);
					}
				} catch (ClassNotFoundException | SQLException e) {
					log.error(e.getMessage(), e);
				}
			});
			executorService.shutdown();
			executorService.awaitTermination(1, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	protected static void tearDownTable(Connection connection, String tableName) throws SQLException {
		execute(connection, "DROP TABLE IF EXISTS " + tableName + ";");
	}

	protected static void tearDownSequence(Connection connection, String sequenceName) throws SQLException {
		execute(connection, "DROP SEQUENCE IF EXISTS " + sequenceName + ";");
	}

	protected static void execute(Connection connection, String query) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(query);
		}
	}

}