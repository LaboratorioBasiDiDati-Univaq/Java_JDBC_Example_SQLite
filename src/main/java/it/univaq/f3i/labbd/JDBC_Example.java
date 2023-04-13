package it.univaq.f3i.labbd;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 *
 * @author giuse
 */
public class JDBC_Example {

    private Query_JDBC query_module;
    private Setup_JDBC setup_module;
    private Connect_JDBC connection_module;

    public JDBC_Example(String connection_string, String username, String password) {
        this.connection_module = new Connect_JDBC(connection_string, username, password);
    }

    public JDBC_Example(String connection_string) {
        this(connection_string, null, null);
    }

    //metodo di utilità che stampa informazioni utili su una SQLException
    //incapsulata in una ApplicationException
    protected void logException(ApplicationException e) {
        Throwable cause = e.getCause();
        System.out.println("\nERRORE: " + e.getMessage());
        if (cause != null) {
            if (cause instanceof SQLException) {
                System.out.println("* SQLState: " + ((SQLException) cause).getSQLState());
                System.out.println("* Codice errore DBMS: " + ((SQLException) cause).getErrorCode());
                System.out.println("* Messaggio errore DBMS: " + ((SQLException) cause).getMessage());
            } else {
                System.out.println("* Causa: " + cause.getMessage());
            }
        }
    }

    public void connect() throws ApplicationException {
        //i due moduli condivideranno la connessione singleton generata dal connection_module
        setup_module = new Setup_JDBC(connection_module.getConnection());
        query_module = new Query_JDBC(connection_module.getConnection());
    }

    public void disconnect() throws ApplicationException {
        connection_module.disconnect();
        //se usiamo una singola connessione prelevata dal connection_module
        //le due disconnect che seguono sono inutili, ma le inseriamo per
        //completezza e sicurezza
        query_module.disconnect();
        setup_module.disconnect();
        setup_module = null;
        query_module = null;
    }

    //eseguiamo tutti i test in sequenza
    public void runQueries() throws ApplicationException {
        System.out.println("\n**** TEST SENZA TRANSAZIONE *************************");
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
        query_module.classifica_marcatori(2020);
        query_module.calendario_campionato(2020);
        query_module.inserisci_partita(cal.getTime(), 1, 1, 2, 1);
        query_module.aggiorna_partita(1, 5, 6);
        query_module.formazione(1, 2020);
        query_module.squadra_appartenenza(1, 2020);
        query_module.controlla_partita(1);

    }

    //eseguiamo tutti i test in sequenza, in una singola transazione
    //in questo modo, se un'operazione fallisce, potremo annullare gli effetti
    //di tutto il blocco
    public void runQueries_withinTransaction() throws ApplicationException {
        System.out.println("\n**** TEST CON TRANSAZIONE ***************************");
        if (setup_module.supports_transactions()) {
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
                query_module.getConnection().setAutoCommit(false);
            } catch (SQLException ex) {
                //se l'autocommit non si può disattivare, solleviamo un'eccezione custom...
                throw new ApplicationException("Problemi di gestione della transazione", ex);
            }
            //qui iniziamo ad operare sul database...
            try {
                //a questo punto il database aprirà una transazione automatica
                //al primo statement che gli viene sottoposto, ma non ne eseguirà
                //il commit
                query_module.inserisci_partita(cal.getTime(), 1, 1, 2, 1);
                //generiamo volontariamente un'eccezione
                query_module.inserisci_partita(cal.getTime(), 1, 1, 2, 1);
                //ora, se tutto è andato bene, finalizziamo le modifiche
                System.out.println("COMMIT DELLE OPERAZIONI ****************************");
                try {
                    query_module.getConnection().commit();
                } catch (SQLException ex) {
                    //se il commit non va a buon fine, solleviamo un'eccezione...
                    throw new ApplicationException("Problemi di gestione della transazione", ex);
                }
            } catch (ApplicationException ex) {
                //qualcosa non è andato... cancelliamo tutte le modifiche effettuate fin qui
                try {
                    System.out.println("ROLLBACK DELLE OPERAZIONI **********************");
                    query_module.getConnection().rollback();
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
                    query_module.getConnection().setAutoCommit(true);
                } catch (SQLException ex) {
                    throw new ApplicationException("Problemi di gestione della transazione", ex);
                }

            }
        } else {
            System.out.println("** NON SUPPORTATO **");
        }
    }

    //eseguiamo i test selezionati
    public void run(boolean create, boolean populate, boolean query) {
        try {
            //per la connection non usiamo la try-with-resources perchè vogliamo
            //avere maggior controllo su come e dove viene chiusa
            connect();
            if (create) {
                setup_module.initDatabase();
            }
            if (populate) {
                setup_module.populateDatabase();
            }
            setup_module.infoDatabase();
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
