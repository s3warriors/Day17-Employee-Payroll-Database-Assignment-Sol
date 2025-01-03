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


            // UC 3: Insert data into employee_payroll table
            String insertQuery = "INSERT INTO employee_payroll (name, salary, start_date) VALUES " +
                    "('John', 50000, '2022-01-01'), " +
                    "('Bill', 60000, '2022-02-01'), " +
                    "('Charlie', 70000, '2023-01-01')";
            executeUpdate(connection, insertQuery);
            System.out.println("Employee payroll data inserted.");

            // UC 4: Retrieve all data from employee_payroll table
            String selectAllQuery = "SELECT * FROM employee_payroll";
            executeQuery(connection, selectAllQuery);


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void executeUpdate(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query);
        }
    }
    private static void executeQuery(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(metaData.getColumnName(i) + ": " + resultSet.getString(i) + " ");
                }
                System.out.println();
            }
        }
    }

}
