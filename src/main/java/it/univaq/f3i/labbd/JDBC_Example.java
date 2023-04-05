package it.univaq.f3i.labbd;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 *
 * @author giuse
 */
public class JDBC_Example {

    private final String password;
    private final String username;
    private final String connection_string;

    public JDBC_Example(String connection_string, String username, String password) {
        this.password = password;
        this.username = username;
        this.connection_string = connection_string;
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

    //connessione al database con DriverManager
    public Connection connect() throws ApplicationException {
        System.out.println("APERTURA CONNESSIONE ***************************");
        try {
            //connessione al database 
            if (username != null && password != null) {
                return DriverManager.getConnection(connection_string, username, password);
            } else {
                return DriverManager.getConnection(connection_string);
            }
        } catch (SQLException ex) {
            //Usiamo un'eccezione user-defined per trasportare e gestire più
            //agevolmente tutte le eccezioni lagate all'uso del database
            throw new ApplicationException("Errore di connessione", ex);
        }
    }

    //inizializziamo il database se non presente
    public void initDatabase() {
        try ( Connection c = connect()) {
            Query_JDBC q = new Query_JDBC(c);
            InputStream resource = getClass().getResourceAsStream("/structure.sql");
            if (resource != null) {
                System.out.println("CREAZIONE DATABASE *****************************");
                q.esegui_script(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
            }
            //
            System.out.println("INFORMAZIONI SUL DATABASE **********************");
            q.analizza_database();
        } catch (SQLException ex) {
            //log degli errori originabili dalla chiamata implicita
            //alla close sulla connection eseguita dal try-with-resources
            logException(new ApplicationException("Problemi di apertura della connessione", ex));
        } catch (IOException ex) {
            logException(new ApplicationException("Errore di lettura del file SQL", ex));
        } catch (ApplicationException ex) {
            logException(ex);
        }
    }

    //svuotiamo e ripopoliamo il database
    public void populateDatabase() {
        try ( Connection c = connect()) {
            Query_JDBC q = new Query_JDBC(c);
            InputStream resource = getClass().getResourceAsStream("/data.sql");
            if (resource != null) {
                System.out.println("POPOLAMENTO DATABASE *****************************");
                q.esegui_script(new String(resource.readAllBytes(), StandardCharsets.UTF_8));
            }
        } catch (SQLException ex) {
            //log degli errori originabili dalla chiamata implicita
            //alla close sulla connection eseguita dal try-with-resources
            logException(new ApplicationException("Problemi di apertura della connessione", ex));
        } catch (IOException ex) {
            logException(new ApplicationException("Errore di lettura del file SQL", ex));
        } catch (ApplicationException ex) {
            logException(ex);
        }
    }

    //eseguiamo tutti i test in sequenza
    public void runQueries() {
        System.out.println("\nTEST SENZA TRANSAZIONE--------------------------");
        //la connessione va sempre chiusa alla fine della sessione
        //a questo scopo, quando possibile, possiamo usare il try-with-resources
        //di Java 7, che include un finally {c.close()} implicito.
        try ( Connection c = connect()) {
            Query_JDBC q = new Query_JDBC(c);
            //prepariamo una data appartenente al calendario 2020
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");
            cal.setTime(sdf.parse("30/09/2020 11:30"));
            q.classifica_marcatori(2020);
            q.calendario_campionato(2020);
            q.inserisci_partita(cal.getTime(), 1, 1, 2, 1);
            q.aggiorna_partita(1, 5, 6);
            q.formazione(1, 2020);
            q.squadra_appartenenza(1, 2020);
            q.controlla_partita(1);
        } catch (ApplicationException ex) {
            //log degli errori originabili dai vari metodi
            //andrebbero gestiti in maniera opportuna!
            logException(ex);
        } catch (SQLException ex) {
            //log degli errori originabili dalla chiamata implicita
            //alla close sulla connection eseguita dal try-with-resources
            logException(new ApplicationException("Problemi di apertura della connessione", ex));
        } catch (ParseException ex) {
            //log degli errori originabili dal parsing della data
            //andrebbero gestiti in maniera opportuna!
            logException(new ApplicationException("Errore interno", ex));
        }
    }

    //eseguiamo tutti i test in sequenza, in una singola transazione
    //in questo modo, se un'operazione fallisce, potremo annullare gli effetti
    //di tutto il blocco
    public void runQueries_withinTransaction() {
        System.out.println("\nTEST CON TRANSAZIONE----------------------------");
        Connection c = null;
        //non usiamo la try with resources, perchè la connessione ci serve
        //anche nel blocco catch (per la rollback)
        try {
            //prepariamo una data appartenente al calendario 2020
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");
            cal.setTime(sdf.parse("30/9/2020 16:15"));
            //
            //ci connettiamo senza usare un try-with-resources
            c = connect();
            //di default, JDBC usa la modalità autocommit che esegue OGNI STATEMENT
            //in una transazione diversa. Disattiviamola...
            try {
                System.out.println("DISABILITAZIONE AUTOCOMMIT *********************");
                c.setAutoCommit(false);
            } catch (SQLException ex) {
                //se l'autocommit non si può disattivare, solleviamo un'eccezione custom...
                throw new ApplicationException("Problemi di gestione della transazione", ex);
            }
            Query_JDBC q = new Query_JDBC(c);
            //a questo punto il database aprirà una transazione automatica
            //al primo statement che gli viene sottoposto, ma non ne eseguirà
            //il commit
            q.inserisci_partita(cal.getTime(), 1, 1, 2, 1);
            //generiamo volontariamente un'eccezione
            q.inserisci_partita(cal.getTime(), 1, 1, 2, 1);
            //ora, se tutto è andato bene, finalizziamo le modifiche
            System.out.println("COMMIT DELLE OPERAZIONI ****************************");
            try {
                c.commit();
            } catch (SQLException ex) {
                //se il commit non va a buon fine, solleviamo un'eccezione custom...
                throw new ApplicationException("Problemi di gestione della transazione", ex);
            }
        } catch (ApplicationException ex) {
            //log degli errori originabili dai vari metodi
            //andrebbero gestiti in maniera opportuna!
            logException(ex);
            //qualcosa non è andato... cancelliamo tutte le modifiche effettuate fin qui
            try {
                if (c != null) {
                    System.out.println("ROLLBACK DELLE OPERAZIONI ***********************");
                    c.rollback();
                }
            } catch (SQLException ex1) {
                //log degli errori originabili dalla rollback
                //andrebbero gestiti in maniera opportuna!
                logException(new ApplicationException("Problemi di rollback sulla connessione", ex));
            }
        } catch (ParseException ex) {
            //log degli errori originabili dal parsing della data
            //andrebbero gestiti in maniera opportuna!
            logException(new ApplicationException("Errore interno", ex));
        } finally {
            //chiusura della connessione
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException ex) {
                    //log degli errori originabili dalla chiamata alla close
                    logException(new ApplicationException("Problemi di apertura della connessione", ex));
                }
            }
        }
    }

}
