DELIMITER ;
-- modifiche necessarie rispetto alla versione MySQL:
-- AUTO_INCREMENT -> AUTOINCREMENT (rigorosamente dopo PRIMARY KEY)
-- le PRIMARY KEY non possono essere UNSIGNED

DROP TABLE IF EXISTS iscritta;
DROP TABLE IF EXISTS dirige;
DROP TABLE IF EXISTS segna;
DROP TABLE IF EXISTS gioca;
DROP TABLE IF EXISTS formazione;
DROP TABLE IF EXISTS partita;
DROP TABLE IF EXISTS campionato;
DROP TABLE IF EXISTS squadra;
DROP TABLE IF EXISTS giocatore;
DROP TABLE IF EXISTS arbitro;
DROP TABLE IF EXISTS luogo;

CREATE TABLE campionato (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    anno SMALLINT UNSIGNED NOT NULL,
    CONSTRAINT campionato_distinto UNIQUE (nome , anno),
    CONSTRAINT controllo_anno CHECK (anno > 1900 AND anno < 2500)
);

CREATE TABLE squadra (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    citta VARCHAR(100) NOT NULL,
    CONSTRAINT squadra_distinta UNIQUE (nome , citta)
);

CREATE TABLE giocatore (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    cognome VARCHAR(50) NOT NULL,
    luogoNascita VARCHAR(100) NOT NULL,
    dataNascita DATE NOT NULL,
    CONSTRAINT giocatore_distinto UNIQUE (nome , cognome , dataNascita , luogoNascita)
);

CREATE TABLE arbitro (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    CF CHAR(16) NOT NULL UNIQUE,
    nome VARCHAR(50) NOT NULL,
    cognome VARCHAR(50) NOT NULL
);

CREATE TABLE luogo (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    citta VARCHAR(100) NOT NULL,
    CONSTRAINT luogo_distinto UNIQUE (nome , citta)
);

CREATE TABLE partita (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    `data` DATETIME NOT NULL,
    ID_squadra_1 INTEGER UNSIGNED NOT NULL,
    ID_squadra_2 INTEGER UNSIGNED NOT NULL,
    punti_squadra_1 SMALLINT UNSIGNED DEFAULT 0,
    punti_squadra_2 SMALLINT UNSIGNED DEFAULT 0,
    ID_luogo INTEGER UNSIGNED NOT NULL,
    ID_campionato INTEGER UNSIGNED NOT NULL,
    CONSTRAINT partita_distinta UNIQUE (`data` , ID_squadra_1 , ID_squadra_2 , ID_campionato),
    CONSTRAINT partita_squadra_1 FOREIGN KEY (ID_squadra_1)
        REFERENCES squadra (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT partita_squadra_2 FOREIGN KEY (ID_squadra_2)
        REFERENCES squadra (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT partita_luogo FOREIGN KEY (ID_luogo)
        REFERENCES luogo (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT partita_campionato FOREIGN KEY (ID_campionato)
        REFERENCES campionato (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE
);

CREATE TABLE formazione (
    anno SMALLINT UNSIGNED NOT NULL,
    numero SMALLINT UNSIGNED NOT NULL,
    ID_squadra INTEGER UNSIGNED NOT NULL,
    ID_giocatore INTEGER UNSIGNED NOT NULL,
    PRIMARY KEY (anno , ID_squadra , ID_giocatore),
    CONSTRAINT formazione_squadra FOREIGN KEY (ID_squadra)
        REFERENCES squadra (ID)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT formazione_giocatore FOREIGN KEY (ID_giocatore)
        REFERENCES giocatore (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE
);

CREATE TABLE gioca (
    minuto_iniziale SMALLINT UNSIGNED NOT NULL DEFAULT 1,
    minuto_finale SMALLINT UNSIGNED NOT NULL,
    ID_giocatore INTEGER UNSIGNED NOT NULL,
    ID_partita INTEGER UNSIGNED NOT NULL,
    PRIMARY KEY (minuto_iniziale , ID_giocatore , ID_partita),
    CONSTRAINT gioca_giocatore FOREIGN KEY (ID_giocatore)
        REFERENCES giocatore (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT gioca_partita FOREIGN KEY (ID_partita)
        REFERENCES partita (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE
);

CREATE TABLE segna (
    minuto SMALLINT UNSIGNED NOT NULL,
    ID_giocatore INTEGER UNSIGNED NOT NULL,
    ID_partita INTEGER UNSIGNED NOT NULL,
    punti TINYINT NOT NULL DEFAULT 1,
    PRIMARY KEY (minuto , ID_giocatore , ID_partita),
    CONSTRAINT segna_giocatore FOREIGN KEY (ID_giocatore)
        REFERENCES giocatore (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT segna_partita FOREIGN KEY (ID_partita)
        REFERENCES partita (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE
);

CREATE TABLE dirige (
    ID_partita INTEGER UNSIGNED NOT NULL,
    ID_arbitro INTEGER UNSIGNED NOT NULL,
    PRIMARY KEY (ID_partita , ID_arbitro),
    CONSTRAINT direzione_partita FOREIGN KEY (ID_partita)
        REFERENCES partita (ID)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT direzione_arbitro FOREIGN KEY (ID_arbitro)
        REFERENCES arbitro (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE
);

CREATE TABLE iscritta (
    ID_campionato INTEGER UNSIGNED NOT NULL,
    ID_squadra INTEGER UNSIGNED NOT NULL,
    PRIMARY KEY (ID_campionato , ID_squadra),
    CONSTRAINT iscritta_campionato FOREIGN KEY (ID_campionato)
        REFERENCES campionato (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT iscritta_quadra FOREIGN KEY (ID_squadra)
        REFERENCES squadra (ID)
        ON DELETE CASCADE ON UPDATE CASCADE
);

