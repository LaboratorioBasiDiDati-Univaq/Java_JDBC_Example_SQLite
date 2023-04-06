package it.univaq.f3i.labbd;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author giuse
 */
public class JDBC_Example {

    private final String password;
    private final String username;
    private final String connection_string;
    private Connection connection;
    private Query_JDBC query_interface;

    public JDBC_Example(String connection_string, String username, String password) {
        this.password = password;
        this.username = username;
        this.connection_string = connection_string;
        this.connection = null;
    }

    public JDBC_Example(String connection_string) {
        this(connection_string, null, null);
    }

    //metodo di utilità che stampa informazioni utili su una SQLException
    //incapsulata in una ApplicationException
    protected void logException(ApplicationException e) {
        Throwable cause = e.getCause();
        System.err.println("ERRORE: " + e.getMessage());
        if (cause != null) {
            if (cause instanceof SQLException) {
                System.err.println("* SQLState: " + ((SQLException) cause).getSQLState());
                System.err.println("* Codice errore DBMS: " + ((SQLException) cause).getErrorCode());
                System.err.println("* Messaggio errore DBMS: " + ((SQLException) cause).getMessage());
            } else {
                System.err.println("* Causa: " + cause.getMessage());
            }
        }
    }

    //connessione al database
    public void connect() throws ApplicationException {
        System.out.println("APERTURA CONNESSIONE ***************************");
        try {
            //connessione al database 
            if (username != null && password != null) {
                this.connection = DriverManager.getConnection(connection_string, username, password);
            } else {
                this.connection = DriverManager.getConnection(connection_string);
            }
            query_interface = new Query_JDBC(this.connection);
        } catch (SQLException ex) {
            //Usiamo un'eccezione user-defined per trasportare e gestire più
            //agevolmente tutte le eccezioni lagate all'uso del database
            throw new ApplicationException("Errore di connessione", ex);
        }
    }

    //disconnessione dal database
    public void disconnect() throws ApplicationException {
        if (connection != null) {
            try {
                System.out.println("CHIUSURA CONNESSIONE ***************************");
                connection.close();
            } catch (SQLException ex) {
                throw new ApplicationException("Errore di disconnessione", ex);
            }
        }
    }

    //stampiamo le informazioni sul database corrente
    public void infoDatabase() throws ApplicationException {
        System.out.println("INFORMAZIONI SUL DATABASE **********************");
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            System.out.println("Nome DBMS: " + databaseMetaData.getDatabaseProductName());
            System.out.println("\tVersione: " + databaseMetaData.getDatabaseProductVersion());
            System.out.println("\tDriver: " + databaseMetaData.getDriverName());
            System.out.println("\t\tVersione: " + databaseMetaData.getDriverVersion());
            System.out.println("\tNome utente: " + databaseMetaData.getUserName());
            System.out.println("\tCaratteristiche: ");
            System.out.println("\t\tOUTER JOIN: " + databaseMetaData.supportsOuterJoins());
            System.out.println("\t\tGROUP BY: " + databaseMetaData.supportsGroupBy());
            System.out.println("\t\tORDER BY con espressioni: " + databaseMetaData.supportsExpressionsInOrderBy());
            System.out.println("\t\tUNION: " + databaseMetaData.supportsUnion());
            System.out.println("\t\tSubqueries correlate: " + databaseMetaData.supportsCorrelatedSubqueries());
            System.out.println("\t\tSubqueries con confronti: " + databaseMetaData.supportsSubqueriesInComparisons());
            System.out.println("\t\tSubqueries con EXITS: " + databaseMetaData.supportsSubqueriesInExists());
            System.out.println("\t\tSubqueries con IN: " + databaseMetaData.supportsSubqueriesInIns());
            System.out.println("\t\tStored Procedures: " + databaseMetaData.supportsStoredProcedures());
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
                System.out.println("CREAZIONE DATABASE *****************************");
                query_interface.esegui_script(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
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
                System.out.println("POPOLAMENTO DATABASE *****************************");
                query_interface.esegui_script(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
            }
        } catch (IOException ex) {
            throw new ApplicationException("Errore di lettura del file SQL", ex);
        }
    }

    //eseguiamo tutti i test in sequenza
    public void runQueries() throws ApplicationException {
        System.out.println("\nTEST SENZA TRANSAZIONE--------------------------");
        Calendar cal = Calendar.getInstance();
        try {
            //prepariamo una data appartenente al calendario 2020
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");
            cal.setTime(sdf.parse("30/9/2020 16:15"));
        } catch (ParseException ex) {
            //log degli errori originabili dal parsing della data
            //andrebbero gestiti in maniera opportuna!
            throw new ApplicationException("Errore interno", ex);
        }
        query_interface.classifica_marcatori(2020);
        query_interface.calendario_campionato(2020);
        query_interface.inserisci_partita(cal.getTime(), 1, 1, 2, 1);
        query_interface.aggiorna_partita(1, 5, 6);
        query_interface.formazione(1, 2020);
        query_interface.squadra_appartenenza(1, 2020);
        query_interface.controlla_partita(1);

    }

    //eseguiamo tutti i test in sequenza, in una singola transazione
    //in questo modo, se un'operazione fallisce, potremo annullare gli effetti
    //di tutto il blocco
    public void runQueries_withinTransaction() throws ApplicationException {
        System.out.println("\nTEST CON TRANSAZIONE----------------------------");
        Calendar cal = Calendar.getInstance();
        try {
            //prepariamo una data appartenente al calendario 2020
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");
            cal.setTime(sdf.parse("30/9/2020 16:15"));
        } catch (ParseException ex) {
            //log degli errori originabili dal parsing della data
            //andrebbero gestiti in maniera opportuna!
            throw new ApplicationException("Errore interno", ex);
        }
        //di default, JDBC usa la modalità autocommit che esegue OGNI STATEMENT
        //in una transazione diversa. Disattiviamola...
        try {
            System.out.println("DISABILITAZIONE AUTOCOMMIT *********************");
            this.connection.setAutoCommit(false);
        } catch (SQLException ex) {
            //se l'autocommit non si può disattivare, solleviamo un'eccezione custom...
            throw new ApplicationException("Problemi di gestione della transazione", ex);
        }
        //qui iniziamo ad operare sul database...
        try {
            //a questo punto il database aprirà una transazione automatica
            //al primo statement che gli viene sottoposto, ma non ne eseguirà
            //il commit
            query_interface.inserisci_partita(cal.getTime(), 1, 1, 2, 1);
            //generiamo volontariamente un'eccezione
            query_interface.inserisci_partita(cal.getTime(), 1, 1, 2, 1);
            //ora, se tutto è andato bene, finalizziamo le modifiche
            System.out.println("COMMIT DELLE OPERAZIONI ****************************");
            try {
                this.connection.commit();
            } catch (SQLException ex) {
                //se il commit non va a buon fine, solleviamo un'eccezione...
                throw new ApplicationException("Problemi di gestione della transazione", ex);
            }
        } catch (ApplicationException ex) {
            //qualcosa non è andato... cancelliamo tutte le modifiche effettuate fin qui
            try {
                System.out.println("ROLLBACK DELLE OPERAZIONI **********************");
                this.connection.rollback();
                //propaghiamo all'esterno l'eccezione dopo il rollback
                throw ex;
            } catch (SQLException ex1) {
                //log degli errori originabili dalla rollback
                throw new ApplicationException("Problemi di rollback sulla connessione", ex);
            }
        } finally {
            //alla fine rimettiamo sempre tutto a posto, riabilitando l'autocommit...
            try {
                System.out.println("ABILITAZIONE AUTOCOMMIT ************************");
                this.connection.setAutoCommit(true);
            } catch (SQLException ex) {
                throw new ApplicationException("Problemi di gestione della transazione", ex);
            }

        }
    }

    //eseguiamo i test selezionati
    public void run(boolean create, boolean populate, boolean query) {
        try {
            //per la connection non usiamo la try-with-resources perchè vogliamo
            //avere maggior controllo su come e dove viene chiusa
            connect();
            if (create) {
                initDatabase();
            }
            if (populate) {
                populateDatabase();
            }
            infoDatabase();
            if (query) {
                runQueries();
                runQueries_withinTransaction();
            }
        } catch (ApplicationException ex) {
            //log degli errori originabili dai vari metodi
            //andrebbero gestiti in maniera opportuna!
            logException(ex);
        } finally {
            //ci assicuriamo che allòa fine del programma la connessione venga chiusa
            try {
                disconnect();
            } catch (ApplicationException ex) {
                logException(ex);
            }
        }
    }
}
