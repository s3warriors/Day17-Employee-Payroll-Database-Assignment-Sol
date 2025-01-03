import java.sql.*;

public class PayrollService {

    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/";
    private static final String DB_NAME = "payroll_service";
    private static final String USER = "root";
    private static final String PASSWORD = "password";

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
            System.out.println("Connected to MySQL server.");

            // UC 1: Create payroll_service database
            executeUpdate(connection, "CREATE DATABASE IF NOT EXISTS " + DB_NAME);
            System.out.println("Database created or already exists.");
            executeUpdate(connection, "USE " + DB_NAME);


            // UC 2: Create employee_payroll table
            String createTableQuery = """
                CREATE TABLE IF NOT EXISTS employee_payroll (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    salary DOUBLE,
                    start_date DATE
                )
            """;
            executeUpdate(connection, createTableQuery);
            System.out.println("Employee payroll table created.");


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void executeUpdate(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query);
        }
    }

}
