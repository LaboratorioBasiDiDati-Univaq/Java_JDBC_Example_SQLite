DELIMITER ;
-- modifiche necessarie rispetto alla versione MySQL:
-- AUTO_INCREMENT -> AUTOINCREMENT (rigorisamente dopo PRIMARY KEY)
-- le PRIMARY KEY non possono essere UNSIGNED

CREATE TABLE IF NOT EXISTS campionato (
    ID INTEGER  PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    anno SMALLINT UNSIGNED NOT NULL,
    CONSTRAINT campionato_distinto UNIQUE (nome , anno),
    CONSTRAINT controllo_anno CHECK (anno > 1900 AND anno < 2500)
);

CREATE TABLE IF NOT EXISTS squadra (
    ID INTEGER  PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    citta VARCHAR(100) NOT NULL,
    CONSTRAINT squadra_distinta UNIQUE (nome , citta)
);

CREATE TABLE IF NOT EXISTS giocatore (
    ID INTEGER  PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    cognome VARCHAR(50) NOT NULL,
    luogoNascita VARCHAR(100) NOT NULL,
    dataNascita DATE NOT NULL,
    CONSTRAINT giocatore_distinto UNIQUE (nome , cognome , dataNascita , luogoNascita)
);

CREATE TABLE IF NOT EXISTS arbitro (
    ID INTEGER  PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    cognome VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS luogo (
    ID INTEGER  PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    citta VARCHAR(100) NOT NULL,
    CONSTRAINT luogo_distinto UNIQUE (nome , citta)
);

CREATE TABLE IF NOT EXISTS partita (
    ID INTEGER  PRIMARY KEY AUTOINCREMENT,
    `data` DATETIME NOT NULL,
    ID_squadra_1 INTEGER NOT NULL,
    ID_squadra_2 INTEGER NOT NULL,
    punti_squadra_1 SMALLINT UNSIGNED DEFAULT 0,
    punti_squadra_2 SMALLINT UNSIGNED DEFAULT 0,
    ID_luogo INTEGER NOT NULL,
    ID_campionato INTEGER NOT NULL,
    CONSTRAINT partita_distinta UNIQUE (`data` , ID_squadra_1 , ID_squadra_2 , ID_campionato),
    CONSTRAINT partita_squadra1 FOREIGN KEY (ID_squadra_1)
        REFERENCES squadra (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT partita_squadra2 FOREIGN KEY (ID_squadra_2)
        REFERENCES squadra (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT partita_luogo FOREIGN KEY (ID_luogo)
        REFERENCES luogo (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT partita_campionato FOREIGN KEY (ID_campionato)
        REFERENCES campionato (ID)
        ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS formazione (
    anno SMALLINT UNSIGNED NOT NULL,
    numero SMALLINT UNSIGNED NOT NULL,
    ID_squadra INTEGER NOT NULL,
    ID_giocatore INTEGER NOT NULL,
    PRIMARY KEY (anno , ID_squadra , ID_giocatore),
    CONSTRAINT formazione_squadra FOREIGN KEY (ID_squadra)
        REFERENCES squadra (ID)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT formazione_giocatore FOREIGN KEY (ID_giocatore)
        REFERENCES giocatore (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS segna (
    minuto SMALLINT UNSIGNED NOT NULL,
    ID_giocatore INTEGER NOT NULL,
    ID_partita INTEGER NOT NULL,
    punti TINYINT NOT NULL DEFAULT 1,
    PRIMARY KEY (minuto , ID_giocatore , ID_partita),
    CONSTRAINT segna_giocatore FOREIGN KEY (ID_giocatore)
        REFERENCES giocatore (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT segna_partita FOREIGN KEY (ID_partita)
        REFERENCES partita (ID)
        ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS direzione (
    ID_partita INTEGER NOT NULL,
    ID_arbitro INTEGER NOT NULL,
    PRIMARY KEY (ID_partita , ID_arbitro),
    CONSTRAINT direzione_partita FOREIGN KEY (ID_partita)
        REFERENCES partita (ID)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT direzione_arbitro FOREIGN KEY (ID_arbitro)
        REFERENCES arbitro (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE
);