CREATE TABLE if not exists campionato (
    ID INTEGER  PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    anno SMALLINT UNSIGNED NOT NULL,
    CONSTRAINT campionato_distinto UNIQUE (nome , anno),
    constraint controllo_anno CHECK(anno>1900 and anno<2500)
);

CREATE TABLE if not exists squadra (
    ID INTEGER  PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    citta VARCHAR(100) NOT NULL,
    CONSTRAINT squadra_distinta UNIQUE (nome , citta)
);

CREATE TABLE if not exists giocatore (
    ID INTEGER  PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    cognome VARCHAR(50) NOT NULL,
    luogoNascita VARCHAR(100) NOT NULL,
    dataNascita DATE NOT NULL,
    CONSTRAINT giocatore_distinto UNIQUE (nome , cognome , dataNascita , luogoNascita)
);

CREATE TABLE if not exists arbitro (
    CF CHAR(16) PRIMARY KEY,
    nome VARCHAR(50) NOT NULL,
    cognome VARCHAR(50) NOT NULL
);

CREATE TABLE if not exists luogo (
    ID INTEGER  PRIMARY KEY AUTOINCREMENT,
    nome VARCHAR(50) NOT NULL,
    citta VARCHAR(100) NOT NULL,
    CONSTRAINT luogo_distinto UNIQUE (nome , citta)
);

CREATE TABLE if not exists partita (
    ID INTEGER  PRIMARY KEY AUTOINCREMENT,
    `data` DATETIME NOT NULL,
    ID_squadra_1 INTEGER UNSIGNED NOT NULL,
    ID_squadra_2 INTEGER UNSIGNED NOT NULL,
    punti_squadra_1 SMALLINT UNSIGNED DEFAULT 0,
    punti_squadra_2 SMALLINT UNSIGNED DEFAULT 0,
    ID_luogo INTEGER UNSIGNED NOT NULL,
    ID_campionato INTEGER UNSIGNED NOT NULL,
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
        ON DELETE NO ACTION ON UPDATE CASCADE
);

CREATE TABLE if not exists formazione (
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
        ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE if not exists segna (
    minuto SMALLINT UNSIGNED NOT NULL,
    ID_giocatore INTEGER UNSIGNED NOT NULL,
    ID_partita INTEGER UNSIGNED NOT NULL,
    tipo char(3) NULL,
    PRIMARY KEY (minuto , ID_giocatore , ID_partita),
    CONSTRAINT segna_giocatore FOREIGN KEY (ID_giocatore)
        REFERENCES giocatore (ID)
        ON DELETE NO ACTION ON UPDATE CASCADE,
    CONSTRAINT segna_partita FOREIGN KEY (ID_partita)
        REFERENCES partita (ID)
        ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE if not exists direzione (
    ID_partita INTEGER UNSIGNED NOT NULL,
    CF_arbitro CHAR(16) NOT NULL,
    PRIMARY KEY (ID_partita , CF_arbitro),
    CONSTRAINT direzione_partita FOREIGN KEY (ID_partita)
        REFERENCES partita (ID)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT direzione_arbitro FOREIGN KEY (CF_arbitro)
        REFERENCES arbitro (CF)
        ON DELETE NO ACTION ON UPDATE CASCADE
);