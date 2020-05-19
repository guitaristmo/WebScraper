import java.io.IOException;
import java.sql.*;
import java.util.Set;

public class DBConnectivity {

    private String ConnectionURL;

    DBConnectivity(String URL) {
        ConnectionURL = URL;
    }

    void HashSetToDB(Set<String> toTransfer) {
        try {
            Connection conn = DriverManager.getConnection(ConnectionURL);
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE WebScrapedEmails (Email CHAR(100))");

            final int EMAILS_PER_INSERT = 1000;                                     //6342
            final int LAST_INSERT_SIZE = toTransfer.size() % EMAILS_PER_INSERT;     // 342
            final int LAST_INSERT_NUM = toTransfer.size() / EMAILS_PER_INSERT;      //6
            int insertCounter = 0;
            int emailNum = 0;
            StringBuilder sqlInsertStmt = new StringBuilder();
            String insertQueryBase = "INSERT INTO WebScrapedEmails (Email) VALUES ";
            sqlInsertStmt.append(insertQueryBase);

            for (String emailAddress : toTransfer) {
                emailNum++;
                if (emailNum <= EMAILS_PER_INSERT)
                    sqlInsertStmt.append("('" + emailAddress + "'), ");
                if (emailNum == EMAILS_PER_INSERT) {
                    CropAndExecuteSQL(sqlInsertStmt, stmt);
                    sqlInsertStmt.delete(0, sqlInsertStmt.length());
                    sqlInsertStmt.append(insertQueryBase);
                    insertCounter++;
                    emailNum = 0;
                }
                if (insertCounter == LAST_INSERT_NUM && emailNum == LAST_INSERT_SIZE)
                    CropAndExecuteSQL(sqlInsertStmt, stmt);
            }
            conn.close();
        } catch (Exception ex) {
            System.out.println("ERROR: " + ex.getMessage());
        }
    }

    private void CropAndExecuteSQL(StringBuilder sqlInsertStmt, Statement stmt) throws SQLException {
        sqlInsertStmt.deleteCharAt(sqlInsertStmt.length() - 1);
        sqlInsertStmt.deleteCharAt(sqlInsertStmt.length() - 1);
        stmt.execute(sqlInsertStmt.toString());
    }

    private void PrintWebScrapedEmailsTable() {
        try {
            Connection conn = DriverManager.getConnection(ConnectionURL);
            Statement stmt = conn.createStatement();
            String sqlStatement = "SELECT * FROM Emails";
            ResultSet result = stmt.executeQuery(sqlStatement);
            int counter = 1;
            while (result.next()) {
                System.out.print(counter);
                System.out.println(result.getString(1));
                counter++;
            }
            conn.close();
        } catch (Exception ex) {
            System.out.println("ERROR: " + ex.getMessage());
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        DBConnectivity verifyTableIsFull = new DBConnectivity("jdbc:sqlserver://spring2019touro.cbjmpwcdjfmq.us-east-1.rds.amazonaws.com;"
                + "database=Ioffe364;"
                + "user=++++++++;"
                + "password=+++++++++;"
                + "encrypt=false;"
                + "trustServerCertificate=false;"
                + "loginTimeout=30;");
        verifyTableIsFull.PrintWebScrapedEmailsTable();
    }
}