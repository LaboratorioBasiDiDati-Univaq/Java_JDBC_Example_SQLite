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
 * Il codice è può ricreare il database e popolarlo. Trattandosi di un database
 * SQLite, questo verrà creato come file nella working directory 
 * dell'applicazione. Basterà cancellare questo file per ripartire da un
 * ambiente "pulito".
 * 
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
