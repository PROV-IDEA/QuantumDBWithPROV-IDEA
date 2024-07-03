package prov.idea.evaluation;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ConnectionUtils {
	static {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	private static final String url = getProperty("url");
	public static String getUrl() {
		return url;
	}

	public static String getUser() {
		return user;
	}

	public static String getPwd() {
		return pwd;
	}
	
	public static String getDriver() {
		return driver;
	}


	private static final String user = getProperty("user");
	private static final String pwd = getProperty("pwd");
	private static final String driver = getProperty("driver");

	public static String getProperty(String clave) {
		String valor = null;
		try {
			Properties props = new Properties();
			DataInputStream prIS = new DataInputStream(
					new FileInputStream("src/test/resources/prov/idea/evaluation/conexion.properties"));
			props.load(prIS);
			valor = props.getProperty(clave);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return valor;
	}

	protected static Connection createConnection() throws ClassNotFoundException, SQLException {
		return DriverManager.getConnection(url, user, pwd);
	}
	
	protected static String getCatalog() throws SQLException {
		try (Connection connection = DriverManager.getConnection(url, user, pwd)) {
			try (Statement statement = connection.createStatement()) {
				ResultSet resultSet = statement.executeQuery("SELECT current_database();");

				if (resultSet.next()) {
					return resultSet.getString(1);
				}
				throw new IllegalStateException("The database URL you've specified does not specify a catalog.");
			}
		}
	}

}
