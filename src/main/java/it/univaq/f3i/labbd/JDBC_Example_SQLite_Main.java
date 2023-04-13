package it.univaq.f3i.labbd;

/**
 *
 * @author Giuseppe Della Penna
 *
 * Questo esempio lavora sul database "campionati" e richiede che esso sia
 * popolato con i dati e le procedure sviluppate a lezione, nonchè che sia
 * presente nel DBMS un utente specifico (vedi qui sotto) con accesso al
 * database.
 *
 * Il codice è può ricreare il database e popolarlo, ma in questo caso è
 * necessario che l'utente con cui si accede abbia i privilegi globali SUPER
 * (per creare le funzioni) nonchè quelli di creazione tabelle, procedure e
 * foreign key sul datbase campionati
 */
public class JDBC_Example_SQLite_Main extends JDBC_Example {

    private static final String DB_NAME = "campionati";
    private static final String DB_FILENAME = DB_NAME + ".sqlite";
    private static final String CONNECTION_STRING = "jdbc:sqlite:" + DB_FILENAME;

    public JDBC_Example_SQLite_Main() {
        super(CONNECTION_STRING);
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        JDBC_Example_SQLite_Main instance = new JDBC_Example_SQLite_Main();
        instance.run(true, true, true);
    }

}
