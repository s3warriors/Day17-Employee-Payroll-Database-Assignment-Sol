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

            // UC 5: Retrieve salary for a particular employee and employees in a date range
            String selectSalaryQuery = "SELECT salary FROM employee_payroll WHERE name = 'Bill'";
            executeQuery(connection, selectSalaryQuery);

            String selectDateRangeQuery = """
                SELECT * FROM employee_payroll
                WHERE start_date BETWEEN CAST('2022-01-01' AS DATE) AND NOW()
            """;
            executeQuery(connection, selectDateRangeQuery);


            // UC 6: Add gender column and update gender for employees
            String alterTableQuery = "ALTER TABLE employee_payroll ADD COLUMN gender CHAR(1) AFTER name";
            executeUpdate(connection, alterTableQuery);

            String updateGenderQuery = """
                UPDATE employee_payroll 
                SET gender = CASE 
                    WHEN name = 'Bill' THEN 'M' 
                    WHEN name = 'Charlie' THEN 'M' 
                    ELSE 'F' 
                END
            """;
            executeUpdate(connection, updateGenderQuery);
            System.out.println("Gender column added and updated.");

            // UC 7: Perform aggregation queries
            String aggregationQuery = """
                SELECT gender, 
                       SUM(salary) AS total_salary, 
                       AVG(salary) AS avg_salary, 
                       MIN(salary) AS min_salary, 
                       MAX(salary) AS max_salary, 
                       COUNT(*) AS count
                FROM employee_payroll
                GROUP BY gender
            """;
            executeQuery(connection, aggregationQuery);


            // UC 8: Extend employee_payroll table to include phone, address, and department
            String alterTableExtendQuery = """
                ALTER TABLE employee_payroll 
                ADD COLUMN phone VARCHAR(15), 
                ADD COLUMN address VARCHAR(255) DEFAULT 'Not Available',
                ADD COLUMN department VARCHAR(100) NOT NULL
            """;
            executeUpdate(connection, alterTableExtendQuery);
            System.out.println("Employee payroll table extended with new fields.");

            // UC 9: Add payroll details (Basic Pay, Deductions, Taxable Pay, Income Tax, Net Pay)
            String alterTablePayrollDetails = """
                ALTER TABLE employee_payroll 
                ADD COLUMN basic_pay DOUBLE, 
                ADD COLUMN deductions DOUBLE, 
                ADD COLUMN taxable_pay DOUBLE, 
                ADD COLUMN income_tax DOUBLE, 
                ADD COLUMN net_pay DOUBLE
            """;
            executeUpdate(connection, alterTablePayrollDetails);
            System.out.println("Payroll details added to the table.");

            // UC 10: Assign Terissa to Sales and Marketing department
            String insertTerissaQuery = """
                INSERT INTO employee_payroll (name, salary, start_date, gender, department)
                VALUES ('Terissa', 80000, '2023-05-01', 'F', 'Sales and Marketing')
            """;
            executeUpdate(connection, insertTerissaQuery);
            System.out.println("Terissa added to Sales and Marketing department.");

            // UC 11: Create Employee-Department table for Many-to-Many relationships
            String createEmployeeDepartmentTable = """
                CREATE TABLE IF NOT EXISTS employee_department (
                    employee_id INT,
                    department_id INT,
                    PRIMARY KEY (employee_id, department_id),
                    FOREIGN KEY (employee_id) REFERENCES employee_payroll(id),
                    FOREIGN KEY (department_id) REFERENCES departments(id)
                )
            """;
            executeUpdate(connection, createEmployeeDepartmentTable);
            System.out.println("Employee-Department table created.");

            // UC 12: Ensure all previous queries work with the new structure
            executeQuery(connection, selectAllQuery);
            executeQuery(connection, aggregationQuery);

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
