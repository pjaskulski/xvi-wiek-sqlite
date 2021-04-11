package main

var sqlCreateDb string = `
        PRAGMA foreign_keys = ON;
    
        DROP TABLE IF EXISTS sources;
        DROP TABLE IF EXISTS facts;
        DROP TABLE IF EXISTS people;
        DROP TABLE IF EXISTS fact_people;
        DROP TABLE IF EXISTS keywords;
        DROP TABLE IF EXISTS fact_keyword;

        CREATE TABLE facts (
			fact_id INTEGER PRIMARY KEY, 
            number TEXT NOT NULL,
            date TEXT,
            day INTEGER NOT NULL, 
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            title TEXT,
            content TEXT,
            content_twitter TEXT,
            location_id INT,
            image TEXT,
            image_info TEXT,
			FOREIGN KEY (location_id) 
            	REFERENCES locations(location_id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT
        );
        CREATE INDEX idx_facts_date ON facts(year, month, day);

        CREATE TABLE people (people_id INTEGER PRIMARY KEY,
                             name TEXT NOT NULL UNIQUE
        );
        CREATE INDEX idx_people_name ON people(name);

        CREATE TABLE fact_people (
            fact_id INTEGER NOT NULL,
            people_id INTEGER NOT NULL,
            FOREIGN KEY (fact_id) 
                REFERENCES facts(fact_id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT,
            FOREIGN KEY (people_id) 
                REFERENCES people(people_id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT,
            PRIMARY KEY(fact_id, people_id)
        );
        
        CREATE TABLE keywords (
            keyword_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );
        CREATE INDEX idx_keywords_name ON keywords(name);
        
        CREATE TABLE fact_keywords (
            fact_id INTEGER NOT NULL,
            keyword_id INTEGER NOT NULL,
            FOREIGN KEY (fact_id) 
                REFERENCES facts(fact_id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT,
            FOREIGN KEY (keyword_id) 
                REFERENCES keywords(keyword_id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT,
            PRIMARY KEY(fact_id, keyword_id)
        );

		CREATE TABLE locations (
			location_id INTEGER PRIMARY KEY,
			name TEXT,
			geo TEXT
		);
		CREATE INDEX idx_locations_name ON locations(name);

        CREATE TABLE sources (
            source_id INTEGER PRIMARY KEY, 
            fact_id INTEGER NOT NULL, 
            value TEXT,
            url_name TEXT,
            url TEXT,
            FOREIGN KEY (fact_id)
                REFERENCES facts (fact_id) 
                ON UPDATE CASCADE
                ON DELETE RESTRICT   
        );

        CREATE INDEX idx_sources_fact_id ON sources(fact_id);
    `

var sqlInsertPeople string = `
    INSERT INTO people 
        (people_id, name) 
    VALUES (?, ?);
`
var sqlInsertKeyword string = `
    INSERT INTO keywords 
        (keyword_id, name) 
    VALUES (?, ?);
    `
var sqlInsertFact string = `
    INSERT INTO facts 
        (fact_id, number, date, day, month, year, title, content, content_twitter, 
            image, image_info) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`
var sqlInsertSource string = `
    INSERT INTO SOURCES 
        (source_id, fact_id, value, url_name, url) 
    VALUES (?, ?, ?, ?, ?);
`
var sqlInsertFactPeople string = `
    INSERT INTO fact_people 
        (fact_id, people_id) 
    VALUES (?, ?);
`
var sqlInsertFactKeyword string = `
    INSERT INTO fact_keywords 
        (fact_id, keyword_id) 
    VALUES (?, ?);
`
var sqlInsertLocation string = `
    INSERT INTO locations 
        (location_id, name, geo) 
    VALUES (?, ?, ?);
`
var sqlUpdateFact string = `
    UPDATE facts 
	SET location_id = ? 
    WHERE fact_id = ?;
`
