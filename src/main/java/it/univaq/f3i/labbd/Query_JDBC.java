package it.univaq.f3i.labbd;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author Giuseppe Della Penna
 *
 * Questo esempio lavora sul database "campionati" e richiede che esso sia
 * popolato con i dati e le procedure sviluppate a lezione, nonchè che sia
 * presente nel DBMS un utente specifico (vedi qui sotto) con accesso al
 * database
 */
public class Query_JDBC {
    
    private Connection connection;
    public static DateTimeFormatter db_date_fomatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    
    public Query_JDBC(Connection c) {
        this.connection = c;
    }
    
    public void analizza_database() throws ApplicationException {
        
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            
            System.out.println("Nome DBMS: " + databaseMetaData.getDatabaseProductName());
            System.out.println("\tVersione: " + databaseMetaData.getDatabaseProductVersion());
            System.out.println("\tDriver: " + databaseMetaData.getDriverName());
            System.out.println("\t\tVersione: " + databaseMetaData.getDriverVersion());
            System.out.println("\tNume utente: " + databaseMetaData.getUserName());
            System.out.println("\tCaratteristiche: ");
            System.out.println("\t\tOuter Joins: " + databaseMetaData.supportsOuterJoins());
            System.out.println("\t\tGroup by: " + databaseMetaData.supportsGroupBy());
            System.out.println("\t\tSubqueries correlate: " + databaseMetaData.supportsCorrelatedSubqueries());
            System.out.println("\t\tSubqueries in comparisons: " + databaseMetaData.supportsSubqueriesInComparisons());
            System.out.println("\t\tSubqueries in exists: " + databaseMetaData.supportsSubqueriesInExists());
            System.out.println("\t\tSubqueries in ins: " + databaseMetaData.supportsSubqueriesInIns());
            System.out.println("\t\tUnion: " + databaseMetaData.supportsUnion());
            System.out.println("\t\tStored Procedures: " + databaseMetaData.supportsStoredProcedures());
            System.out.println("\t\tTransazioni: " + databaseMetaData.supportsTransactions());            
            System.out.println("\t\tOrder by expressions: " + databaseMetaData.supportsExpressionsInOrderBy());
            System.out.println("\t\tGet generated keys: " + databaseMetaData.supportsGetGeneratedKeys());
            
            
            
            try ( ResultSet schemas = databaseMetaData.getSchemas()) {
                while (schemas.next()) {
                    System.out.println("\t" + schemas.getString("TABLE_SCHEM") + " - " + schemas.getString("TABLE_CATALOG"));
                }
            }
            
            System.out.println("Tabelle nel DB corrente: ");
            
            try ( ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"})) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    System.out.println("\t" + tableName);
                    try ( ResultSet columns = databaseMetaData.getColumns(null, null, tableName, null)) {
                        while (columns.next()) {
                            String columnName = columns.getString("COLUMN_NAME");
                            System.out.println("\t\t" + columnName + " " + JDBCType.valueOf(columns.getInt("DATA_TYPE")).getName() + "(" + columns.getString("COLUMN_SIZE") + ") " + ((columns.getString("IS_NULLABLE").equals("NO")) ? "NOT NULL" : "") + " " + ((columns.getString("IS_AUTOINCREMENT").equals("YES")) ? "AUTO_INCREMENT" : ""));
                        }
                    }
                    try ( ResultSet primaryKeys = databaseMetaData.getPrimaryKeys(null, null, tableName)) {
                        List<String> pkNames = new ArrayList<>();
                        while (primaryKeys.next()) {
                            pkNames.add(primaryKeys.getString("COLUMN_NAME"));
                        }
                        System.out.println("\t\tPRIMARY KEY (" + pkNames.stream().collect(Collectors.joining(",")) + ")");                        
                    }
                }
            }

            /*
            
            
            try ( ResultSet foreignKeys = databaseMetaData.getImportedKeys(null, null, "CUSTOMER_ADDRESS")) {
            while (foreignKeys.next()) {
            String pkTableName = foreignKeys.getString("PKTABLE_NAME");
            String fkTableName = foreignKeys.getString("FKTABLE_NAME");
            String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");
            String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
            }
            }
             */
        } catch (SQLException ex) {
            throw new ApplicationException("Errore di lettura dei metadati", ex);
        }
    }
    //INIT: esegue uno script SQL passato sotto forma di stringa. Usato per inizializzare il database

    public void esegui_script(String script_sql) throws ApplicationException {
        try {
            try ( Statement s = getConnection().createStatement()) {
                s.executeUpdate(script_sql);
            }
        } catch (SQLException ex) {
            throw new ApplicationException("Errore di esecuzione dello script SQL", ex);
        }
    }

    //ESEMPIO 1: esecuzione diretta di query e lettura dei risultati
    public void classifica_marcatori(int anno) throws ApplicationException {
        System.out.println("CLASSIFICA MARCATORI " + anno + "-----------------------");
        //eseguiamo la query
        //notare che creiamo lo statement e il resultset in un try-with-resources
        try ( Statement s = getConnection().createStatement(); //attenzione: in generale sarebbe meglio scrivere le stringhe di SQL
                //sotto forma di costanti (ad esempio a livello classe) e riferirvisi 
                //solo nel codice, per una migliore mantenibilità dei sorgenti
                  ResultSet rs = s.executeQuery("select g.cognome,g.nome, s.nome as squadra, count(*) as punti from\n"
                        + "giocatore g \n"
                        + "	join segna m on (m.ID_giocatore=g.ID)\n"
                        + "	join partita p on (p.ID=m.ID_partita) \n"
                        + "	join campionato c on (p.ID_campionato=c.ID)\n"
                        + "	join formazione f on (f.ID_giocatore=g.ID)\n"
                        + "	join squadra s on (s.ID=f.ID_squadra)\n"
                        + "where (c.anno=f.anno) and c.anno=" + anno + " \n"
                        + "group by g.cognome,g.nome, s.nome\n"
                        + "order by punti desc;"); //PERICOLOSO! Usiamo sempre i PreparedStatement!
                ) { //iteriamo nella lista di record risultanti
            while (rs.next()) {
                //stampiamo le varie colonne di ciascun record, prelevandole col tipo corretto
                System.out.print(rs.getString("nome"));
                System.out.print("\t" + rs.getString("cognome"));
                System.out.print("\t" + rs.getString("squadra"));
                System.out.println("\t" + rs.getInt("punti"));
            }
        } catch (SQLException ex) {
            throw new ApplicationException("Errore di esecuzione della query", ex);
        }
        //s e rs vengono chiusi automaticamente dal try-with-resources
    }

    //ESEMPIO 2: esecuzione di query precompilata con passaggio parametri
    public void calendario_campionato(int anno) throws ApplicationException {
        System.out.println("CALENDARIO CAMPIONATO " + anno + "----------------------");
        //un oggetto-formattatore per le date
        DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
        //precompiliamo la query            
        try ( PreparedStatement s = getConnection().prepareStatement("select s1.nome as squadra1,s2.nome as squadra2,p.data\n"
                + "from campionato c join partita p on (p.ID_campionato=c.ID) join squadra s1 on (p.ID_squadra_1 = s1.ID) join squadra s2 on (p.ID_squadra_2 = s2.ID)\n"
                + "where c.anno=?\n"
                + "order by p.data asc;")) {
            //impostiamo i parametri della query
            s.setInt(1, anno);
            //eseguiamo la query
            //questo try-with-resources senza catch garantisce la chisura di rs al termine del suo uso
            try ( ResultSet rs = s.executeQuery()) {
                //iteriamo nella lista di record risultanti
                while (rs.next()) {
                    //stampiamo le varie colonne di ciascun record, prelevandole col tipo corretto
                    System.out.print(rs.getString("squadra1"));
                    System.out.print("\t" + rs.getString("squadra2"));
                    //una colonna DATE viene estratta con il tipo Java java.sql.Date, una sottoclasse di java.util.Date
                    System.out.println("\t" + df.format(rs.getDate("data")));
                }
            }
        } catch (SQLException ex) {
            throw new ApplicationException("Errore di esecuzione della query", ex);
        }
    }

    //ESEMPIO 3: esecuzione di query di inserimento
    public void inserisci_partita(Date data, int ID_campionato, int ID_squadra_1, int ID_squadra_2, int ID_luogo) throws ApplicationException {
        System.out.println("INSERIMENTO PARTITA " + ID_squadra_1 + "-" + ID_squadra_2 + "---------------------------");
        //precompiliamo la query       
        //il parametro extra dice al driver dove trovare la chiave auto-generata del nuovo record
        try ( PreparedStatement s = getConnection().prepareStatement("insert into partita(ID_campionato, data,ID_squadra_1,ID_squadra_2,ID_luogo) values(?,?,?,?,?)", new String[]{"ID"})) {
            //impostiamo i parametri della query
            s.setInt(1, ID_campionato);
            //la java.util.Date va convertita in java.sql.Timestamp (data+ora) o java.sql.Date (solo data)
            s.setTimestamp(2, new java.sql.Timestamp(data.getTime()));
            s.setInt(3, ID_squadra_1);
            s.setInt(4, ID_squadra_2);
            s.setInt(5, ID_luogo);
            //eseguiamo la query
            int affected = s.executeUpdate();
            //stampiamo il numero di record inseriti
            System.out.println("record inseriti: " + affected);
            //volendo estrarre la chiave auto-generata per i record inseriti...
            try ( ResultSet rs = s.getGeneratedKeys()) {
                while (rs.next()) {
                    //stampiamo le chiavi (i record hanno tante colonne quante sono 
                    //le colonne specificate nel secondo parametro della prepareStatement)
                    System.out.println("chiave generata: " + rs.getInt(1));
                }
            }
        } catch (SQLException ex) {
            throw new ApplicationException("Errore di esecuzione della query", ex);
        }
    }

    //ESEMPIO 4: esecuzione di query di aggiornamento
    public void aggiorna_partita(int ID_partita, int punti_squadra_1, int punti_squadra_2) throws ApplicationException {
        System.out.println("AGGIORNAMENTO PARTITA " + ID_partita + "-------------------------");
        //precompiliamo la query       
        try ( PreparedStatement s = getConnection().prepareStatement("update partita set punti_squadra_1=?, punti_squadra_2=? where ID=?")) {
            //impostiamo i parametri della query
            s.setInt(1, punti_squadra_1);
            s.setInt(2, punti_squadra_2);
            s.setInt(3, ID_partita);
            //eseguiamo la query
            int affected = s.executeUpdate();
            //stampiamo il numero di record modificati
            System.out.println("record modificati: " + affected);
        } catch (SQLException ex) {
            throw new ApplicationException("Errore di esecuzione della query", ex);
        }
    }

    /**
     * @return the connection
     */
    private Connection getConnection() {
        return connection;
    }
    
}
