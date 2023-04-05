package it.univaq.f3i.labbd;

/**
 *
 * @author Giuseppe Della Penna
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
        boolean populate = false;
        //populate = !(args.length>0 && args[0].equals("no"));
        JDBC_Example_SQLite_Main instance = new JDBC_Example_SQLite_Main();
        instance.initDatabase();
        if (populate) instance.populateDatabase();
        instance.runQueries();
        instance.runQueries_withinTransaction();
    }

}
