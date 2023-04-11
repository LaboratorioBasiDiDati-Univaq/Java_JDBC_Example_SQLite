package it.univaq.f3i.labbd;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author giuse
 */
public class Setup_JDBC {

    private final Connection connection;

    public Setup_JDBC(Connection c) {
        this.connection = c;
    }

    //esegue uno script SQL generico passato sotto forma di stringa
    public void esegui_script(String script_sql) throws ApplicationException {
        try {
            try ( Statement s = connection.createStatement()) {
                s.execute(script_sql);
            }
        } catch (SQLException ex) {
            throw new ApplicationException("Errore di esecuzione dello script SQL", ex);
        }
    }

    //stampiamo le informazioni sul database corrente
    public void infoDatabase() throws ApplicationException {
        System.out.println("\n**** INFORMAZIONI SUL DATABASE **********************");
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
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
            System.out.println("\t\tChiamata stored functions: " +  (databaseMetaData.supportsStoredProcedures() && databaseMetaData.supportsStoredFunctionsUsingCallSyntax()));
            System.out.println("\t\tTransazioni: " + databaseMetaData.supportsTransactions());
            System.out.println("\t\tGet generated keys: " + databaseMetaData.supportsGetGeneratedKeys());
            //
            System.out.println("Struttura nel Database corrente (" + connection.getSchema() + "): ");
            try ( ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"})) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    System.out.println("\t" + tableName);
                    try ( ResultSet columns = databaseMetaData.getColumns(null, null, tableName, null)) {
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
                    try ( ResultSet primaryKeys = databaseMetaData.getPrimaryKeys(null, null, tableName)) {
                        List<String> pkNames = new ArrayList<>();
                        while (primaryKeys.next()) {
                            pkNames.add(primaryKeys.getString("COLUMN_NAME"));
                        }
                        System.out.println("\t\tPRIMARY KEY (" + pkNames.stream().collect(Collectors.joining(",")) + ")");
                    }
                    //
                    try ( ResultSet foreignKeys = databaseMetaData.getImportedKeys(null, null, tableName)) {
                        while (foreignKeys.next()) {
                            System.out.print("\t\tFOREIGN KEY " + foreignKeys.getString("FKTABLE_NAME") + "(" + foreignKeys.getString("FKCOLUMN_NAME") + ")");
                            System.out.println(" REFERENCES " + foreignKeys.getString("PKTABLE_NAME") + "(" + foreignKeys.getString("PKCOLUMN_NAME") + ")");
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            throw new ApplicationException("Errore di lettura dei metadati", ex);
        }
    }

    //inizializziamo il database se non presente
    public void initDatabase() throws ApplicationException {
        try {
            InputStream resource = getClass().getResourceAsStream("/structure.sql");
            if (resource != null) {
                System.out.println("\n**** CREAZIONE DATABASE *****************************");
                esegui_script(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
            }
        } catch (IOException ex) {
            throw new ApplicationException("Errore di lettura del file SQL", ex);
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
            throw new ApplicationException("Errore di lettura del file SQL", ex);
        }
    }
}
