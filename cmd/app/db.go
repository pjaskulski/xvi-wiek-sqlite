package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v2"
)

// readFact func - czyta dane wydarzeń historycznych z pliku yaml
func (app *application) readFact(filename string) {
	var result []Fact
	var fact Fact

	name := filenameWithoutExtension(filepath.Base(filename))

	fileBuf, err := ioutil.ReadFile(filename)
	if err != nil {
		app.errorLog.Fatal(err)
	}

	r := bytes.NewReader(fileBuf)
	yamlDec := yaml.NewDecoder(r)

	yamlErr := yamlDec.Decode(&fact)

	for yamlErr == nil {
		/* walidacja danych w strukturze fact (część pól jest wymaganych, brak nie
		   zatrzymuje działania aplikacji, ale jest odnotowywany w logu).
		*/
		err = fact.Validate()
		if err != nil {
			app.errorLog.Println("file:", filepath.Base(filename)+",", "error:", err)
		}

		fact.ContentText = prepareTextStyle(fact.Content, false)

		result = append(result, fact)

		yamlErr = yamlDec.Decode(&fact)
	}

	// jeżeli był błąd w pliku yaml, inny niż koniec pliku to zapis w logu
	if yamlErr != nil && yamlErr.Error() != "EOF" {
		app.errorLog.Println("file:", filepath.Base(filename)+",", "error:", yamlErr)
	}

	numberOfFacts += len(result)

	sort.Slice(result, func(i, j int) bool {
		return result[i].Year < result[j].Year
	})

	app.dataCache[name] = result
}

// loadData - wczytuje podczas startu serwera dane do struktur w pamięci operacyjnej
func (app *application) loadData(path string) error {
	// wydarzenia
	app.infoLog.Printf("Wczytywanie bazy wydarzeń...")
	start := time.Now()

	dataFiles, _ := filepath.Glob(filepath.Join(path, "*-*.yaml"))
	if len(dataFiles) > 0 {
		for _, tFile := range dataFiles {
			app.readFact(tFile)
		}
	}

	elapsed := time.Since(start)
	app.infoLog.Printf("Czas wczytywania danych: %s", elapsed)

	return nil
}

// funkcja zlicza rekordy w tabelach
func (app *application) countRec(database *sql.DB, tableName string) {
	var count int

	row := database.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	err := row.Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	app.infoLog.Printf("Rekordów zapisanych w tabeli '%s': %d", tableName, count)
}

// funkcja tworzy bazę danych w formacie sqlite i wypełnia danymi pobranymi
// wcześniej z plików yaml
func (app *application) createSQLite(filename string) {
	if fileExists(filename) {
		os.Remove(filename)
	}

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_foreign_keys=on", filename))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sqlQuery := `
		PRAGMA foreign_keys = ON;
	
		DROP TABLE IF EXISTS sources;
		DROP TABLE IF EXISTS facts;
		DROP TABLE IF EXISTS people;
		DROP TABLE IF EXISTS fact_people;
		DROP TABLE IF EXISTS keywords;
		DROP TABLE IF EXISTS fact_keyword;

		CREATE TABLE facts (fact_id INTEGER NOT NULL PRIMARY KEY, 
			                number TEXT NOT NULL,
							day INTEGER NOT NULL, 
							month INTEGER NOT NULL,
							year INTEGER NOT NULL,
							title TEXT,
							content TEXT,
							content_twitter TEXT,
							location TEXT,
							geo TEXT,
							image TEXT,
							image_info TEXT
		);
		CREATE INDEX idx_facts_date ON facts(year, month, day);

		CREATE TABLE people (people_id INTEGER NOT NULL PRIMARY KEY,
							 name TEXT
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
			keyword_id INTEGER NOT NULL PRIMARY KEY,
			word TEXT
		);
		CREATE INDEX idx_keywords_word ON keywords(word);
		
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

		CREATE TABLE sources (source_id INTEGER NOT NULL PRIMARY KEY, 
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

	_, err = db.Exec(sqlQuery)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlQuery)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// tabela facts
	sqlInsertFact := `
		insert into facts 
			(fact_id, number, day, month, year, title, content, content_twitter, 
				location, geo, image, image_info) 
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	stmtFact, err := tx.Prepare(sqlInsertFact)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtFact.Close()

	// tabela people
	sqlInsertPeople := `
		insert into people 
			(people_id, name) 
		values (?, ?)
	`
	stmtPeople, err := tx.Prepare(sqlInsertPeople)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtPeople.Close()

	// tabela keywords
	sqlInsertKeyword := `
		insert into keywords 
			(keyword_id, word) 
		values (?, ?)
	`
	stmtKeyword, err := tx.Prepare(sqlInsertKeyword)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtKeyword.Close()

	// tabela fact_people
	sqlInsertFactPeople := `
		insert into fact_people 
			(fact_id, people_id) 
		values (?, ?)
	`
	stmtFactPeople, err := tx.Prepare(sqlInsertFactPeople)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtFactPeople.Close()

	// tabela fact_keywords
	sqlInsertFactKeyword := `
		insert into fact_keywords 
			(fact_id, keyword_id) 
		values (?, ?)
	`
	stmtFactKeyword, err := tx.Prepare(sqlInsertFactKeyword)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtFactKeyword.Close()

	// tabela sources
	sqlInsertSource := `
		insert into sources 
			(source_id, fact_id, value, url_name, url) 
		values (?, ?, ?, ?, ?)
	`
	stmtSource, err := tx.Prepare(sqlInsertSource)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtSource.Close()

	for _, facts := range app.dataCache {
		for _, fact := range facts {
			result, err := stmtFact.Exec(nil, fact.ID, fact.Day, fact.Month, fact.Year,
				fact.Title, fact.ContentText, fact.ContentTwitter, fact.Location,
				fact.Geo, fact.Image, fact.ImageInfo)
			if err != nil {
				log.Fatal(err)
			}

			// id nowego rekordu w tabeli facts
			insertedId, err := result.LastInsertId()
			if err != nil {
				log.Fatal(err)
			}

			// fact.People,
			if fact.People != "" {
				persons := strings.Split(fact.People, ";")
				for _, person := range persons {
					person = strings.TrimSpace(person)

					// weryfikacja czy już nie istnieje w bazie

					resultPeople, err := stmtPeople.Exec(nil, person)
					if err != nil {
						log.Fatal(err)
					}
					peopleInsertedId, err := resultPeople.LastInsertId()
					if err != nil {
						log.Fatal(err)
					}
					_, err = stmtFactPeople.Exec(insertedId, peopleInsertedId)
					if err != nil {
						log.Fatal(err)
					}
				}
			}

			// fact.Keywords
			if fact.Keywords != "" {
				keywords := strings.Split(fact.Keywords, ";")
				for _, keyword := range keywords {
					keyword = strings.TrimSpace(keyword)

					// weryfikacja czy już nie istnieje w bazie

					resultKeyword, err := stmtKeyword.Exec(nil, keyword)
					if err != nil {
						log.Fatal(err)
					}
					keywordInsertedId, err := resultKeyword.LastInsertId()
					if err != nil {
						log.Fatal(err)
					}
					_, err = stmtFactKeyword.Exec(insertedId, keywordInsertedId)
					if err != nil {
						log.Fatal(err)
					}
				}
			}

			for _, source := range fact.Sources {
				_, err := stmtSource.Exec(nil, insertedId, source.Value, source.URLName, source.URL)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}

	tx.Commit()

	// weryfikacja liczby rekordów
	app.countRec(db, "facts")
	app.countRec(db, "sources")
	app.countRec(db, "people")
	app.countRec(db, "keywords")
	app.countRec(db, "fact_people")
	app.countRec(db, "fact_keywords")
}
