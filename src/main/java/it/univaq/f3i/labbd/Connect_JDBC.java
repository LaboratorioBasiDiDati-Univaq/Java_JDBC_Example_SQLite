package it.univaq.f3i.labbd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author giuse
 */
public class Connect_JDBC {

    private Connection connection;

    public Connect_JDBC() {
        this.connection = null;
    }

    public Connection getConnection() {
        return connection;
    }

    //connessione al database
    public Connection connect(String connection_string, String username, String password) throws ApplicationException {
        System.out.println("\n**** APERTURA CONNESSIONE ***************************");
        try {
            //connessione al database 
            if (username != null && password != null) {
                this.connection = DriverManager.getConnection(connection_string, username, password);
            } else {
                this.connection = DriverManager.getConnection(connection_string);
            }
            return this.connection;
        } catch (SQLException ex) {
            //Usiamo un'eccezione user-defined per trasportare e gestire più
            //agevolmente tutte le eccezioni lagate all'uso del database
            throw new ApplicationException("Errore di connessione", ex);
        }
    }

    //disconnessione dal database
    public void disconnect() throws ApplicationException {
        if (this.connection != null) {
            try {
                System.out.println("\n**** CHIUSURA CONNESSIONE ***************************");
                this.connection.close();
            } catch (SQLException ex) {
                throw new ApplicationException("Errore di disconnessione", ex);
            }
        }
    }
}
