package it.univaq.f3i.labbd;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author giuse
 */
public class Setup_JDBC {

    private Connection connection;
    private DatabaseMetaData databaseMetaData;
    private boolean supports_procedures;
    private boolean supports_function_calls;
    private boolean supports_transactions;
    private final SQLScriptRunner_JDBC scriptRunner;

    public Setup_JDBC(Connection c) throws ApplicationException {
        scriptRunner = new SQLScriptRunner_JDBC();
        connect(c);
    }

    public final void connect(Connection c) throws ApplicationException {
        disconnect();
        this.connection = c;
        this.supports_procedures = false;
        this.supports_transactions = false;
        this.supports_function_calls = false;
        try {
            databaseMetaData = connection.getMetaData();
            supports_procedures = connection.getMetaData().supportsStoredProcedures();
            supports_function_calls = supports_procedures && connection.getMetaData().supportsStoredFunctionsUsingCallSyntax();
            supports_transactions = connection.getMetaData().supportsTransactions();
        } catch (SQLException ex) {
            throw new ApplicationException("Errore di lettura dei metadati della connessione", ex);
        }
    }

    public Connection getConnection() {
        return this.connection;
    }

    public DatabaseMetaData getDatabaseMetaData() {
        return databaseMetaData;
    }

    public boolean supports_procedures() {
        return supports_procedures;
    }

    public boolean supports_function_calls() {
        return supports_function_calls;
    }

    public boolean supports_transactions() {
        return supports_transactions;
    }

    public void disconnect() throws ApplicationException {
        try {
            if (this.connection != null && !this.connection.isClosed()) {
                System.out.println("\n**** CHIUSURA CONNESSIONE (modulo setup) ************");
                this.connection.close();
                this.connection = null;
            }
        } catch (SQLException ex) {
            throw new ApplicationException("Errore di disconnessione", ex);
        }
    }

    //esegue uno script SQL generico (ddl o dml) passato sotto forma di stringa
    //per ragioni di performance e sicurezza, esegue l'intero script in una singola transazione
    public void esegui_script(String script_sql) throws ApplicationException {
        boolean originalAutoCommit = true;
        try {
            if (supports_transactions) {
                originalAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
            }
            //
            scriptRunner.runScript(connection, script_sql);
            if (supports_transactions) {
                connection.commit();
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException ex) {
            if (supports_transactions) {
                try {
                    connection.rollback();
                    connection.setAutoCommit(originalAutoCommit);
                } catch (SQLException ex1) {
                    //
                }
            }
            throw new ApplicationException("Errore di esecuzione dello script SQL", ex);
        } catch (ApplicationException ex) {
            if (supports_transactions) {
                try {
                    connection.rollback();
                    connection.setAutoCommit(originalAutoCommit);
                } catch (SQLException ex1) {
                    //
                }
            }
            throw ex;
        }
    }

    //stampiamo le informazioni sul database corrente
    public void infoDatabase() throws ApplicationException {
        System.out.println("\n**** INFORMAZIONI SUL DATABASE **********************");
        try {
            System.out.println("Nome DBMS: " + databaseMetaData.getDatabaseProductName());
            System.out.println("\tVersione: " + databaseMetaData.getDatabaseProductVersion());
            System.out.println("\tDriver: " + databaseMetaData.getDriverName());
            System.out.println("\t\tVersione: " + databaseMetaData.getDriverVersion());
            System.out.println("\tNome utente: " + databaseMetaData.getUserName());
            System.out.println("\tCaratteristiche: ");
            System.out.println("\t\tANSI92 Entry SQL: " + databaseMetaData.supportsANSI92EntryLevelSQL());
            System.out.println("\t\tANSI92 Intermediate SQL: " + databaseMetaData.supportsANSI92IntermediateSQL());
            System.out.println("\t\tANSI92 Full SQL: " + databaseMetaData.supportsANSI92FullSQL());
            System.out.println("\t\tALTER TABLE ADD COLUMN: " + databaseMetaData.supportsAlterTableWithAddColumn());
            System.out.println("\t\tALTER TABLE DROP COLUMN: " + databaseMetaData.supportsAlterTableWithDropColumn());
            System.out.println("\t\tAlias colonne (AS): " + databaseMetaData.supportsColumnAliasing());
            System.out.println("\t\tNOT NULL: " + databaseMetaData.supportsNonNullableColumns());
            System.out.println("\t\tOUTER JOIN: " + databaseMetaData.supportsOuterJoins());
            System.out.println("\t\tGROUP BY: " + databaseMetaData.supportsGroupBy());
            System.out.println("\t\tORDER BY con espressioni: " + databaseMetaData.supportsExpressionsInOrderBy());
            System.out.println("\t\tUNION: " + databaseMetaData.supportsUnion());
            System.out.println("\t\tUNION ALL: " + databaseMetaData.supportsUnionAll());
            System.out.println("\t\tSubqueries correlate: " + databaseMetaData.supportsCorrelatedSubqueries());
            System.out.println("\t\tSubqueries con confronti: " + databaseMetaData.supportsSubqueriesInComparisons());
            System.out.println("\t\tSubqueries con EXITS: " + databaseMetaData.supportsSubqueriesInExists());
            System.out.println("\t\tSubqueries con IN: " + databaseMetaData.supportsSubqueriesInIns());
            System.out.println("\t\tStored Procedures: " + databaseMetaData.supportsStoredProcedures());
            System.out.println("\t\tChiamata stored functions: " + (databaseMetaData.supportsStoredProcedures() && databaseMetaData.supportsStoredFunctionsUsingCallSyntax()));
            System.out.println("\t\tTransazioni: " + databaseMetaData.supportsTransactions());
            System.out.println("\t\tGet generated keys: " + databaseMetaData.supportsGetGeneratedKeys());
            //
            System.out.println("Struttura del Database corrente (" + connection.getCatalog() + "): ");
            try ( ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"})) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    System.out.println("\tTABLE " + tableName);
                    try ( ResultSet columns = databaseMetaData.getColumns(connection.getCatalog(), null, tableName, null)) {
                        while (columns.next()) {
                            String columnName = columns.getString("COLUMN_NAME");
                            System.out.print("\t\t" + columnName);
                            System.out.print(" " + JDBCType.valueOf(columns.getInt("DATA_TYPE")).getName() + "(" + columns.getString("COLUMN_SIZE") + ")");
                            System.out.print(" [" + columns.getString("TYPE_NAME") + "]");
                            System.out.print((columns.getString("IS_NULLABLE").equals("NO")) ? " NOT NULL" : "");
                            System.out.println((columns.getString("IS_AUTOINCREMENT").equals("YES")) ? " AUTO_INCREMENT" : "");
                        }
                    }
                    //
                    try ( ResultSet primaryKeys = databaseMetaData.getPrimaryKeys(connection.getCatalog(), null, tableName)) {
                        List<String> pkNames = new ArrayList<>();
                        while (primaryKeys.next()) {
                            pkNames.add(primaryKeys.getString("COLUMN_NAME"));
                        }
                        System.out.println("\t\tPRIMARY KEY (" + pkNames.stream().collect(Collectors.joining(",")) + ")");
                    }
                    //
                    try ( ResultSet foreignKeys = databaseMetaData.getImportedKeys(connection.getCatalog(), null, tableName)) {
                        while (foreignKeys.next()) {
                            System.out.print("\t\tFOREIGN KEY " + foreignKeys.getString("FKTABLE_NAME") + "(" + foreignKeys.getString("FKCOLUMN_NAME") + ")");
                            System.out.print(" REFERENCES " + foreignKeys.getString("PKTABLE_NAME") + "(" + foreignKeys.getString("PKCOLUMN_NAME") + ")");
                            System.out.print(" ON UPDATE " + foreignKeys.getString("DELETE_RULE"));
                            System.out.println(" ON UPDATE " + foreignKeys.getString("UPDATE_RULE"));
                        }
                    }
                }
            }

            try ( ResultSet views = databaseMetaData.getTables(connection.getCatalog(), null, null, new String[]{"VIEW"})) {
                while (views.next()) {
                    String viewName = views.getString("TABLE_NAME");
                    System.out.println("\t VIEW" + viewName);
                    try ( ResultSet columns = databaseMetaData.getColumns(null, null, viewName, null)) {
                        while (columns.next()) {
                            String columnName = columns.getString("COLUMN_NAME");
                            System.out.print("\t\t" + columnName);
                            System.out.println(" " + JDBCType.valueOf(columns.getInt("DATA_TYPE")).getName() + "(" + columns.getString("COLUMN_SIZE") + ")");
                        }
                    }
                }
            }
            if (databaseMetaData.supportsStoredProcedures()) {
                try ( ResultSet procedures = databaseMetaData.getProcedures(connection.getCatalog(), null, null)) {
                    while (procedures.next()) {
                        System.out.println("\tPROCEDURE " + procedures.getString("PROCEDURE_NAME"));
                    }
                }
            }
        } catch (SQLException ex) {
            throw new ApplicationException("Errore di lettura dei metadati", ex);
        }
    }

    //inizializziamo il database tramite gli script SQL forniti
    public void initDatabase() throws ApplicationException {
        try {
            InputStream resource = getClass().getResourceAsStream("/structure.sql");
            if (resource != null) {
                System.out.println("\n**** CREAZIONE DATABASE *****************************");
                esegui_script(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
            }
            if (databaseMetaData.supportsStoredProcedures()) {
                resource = getClass().getResourceAsStream("/procedures.sql");
                if (resource != null) {
                    System.out.println("\n**** DEFINIZIONE PROCEDURE E FUNZIONI ***************");
                    esegui_script(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
                }
            }
            resource = getClass().getResourceAsStream("/triggers.sql");
            if (resource != null) {
                System.out.println("\n**** DEFINIZIONE TRIGGER ****************************");
                esegui_script(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
            }

            resource = getClass().getResourceAsStream("/views.sql");
            if (resource != null) {
                System.out.println("\n**** DEFINIZIONE VISTE ******************************");
                esegui_script(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
            }
        } catch (IOException | SQLException ex) {
            throw new ApplicationException("Errore di esecuzione degli script di inizializzazione", ex);
        }
    }

    //svuotiamo e ripopoliamo il database
    public void populateDatabase() throws ApplicationException {
        try {
            InputStream resource = getClass().getResourceAsStream("/data.sql");
            if (resource != null) {
                System.out.println("\n**** POPOLAMENTO DATABASE ***************************");
                esegui_script(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
            }
        } catch (IOException ex) {
            throw new ApplicationException("Errore di esecuzione degli script di popolamento", ex);
        }
    }
}
