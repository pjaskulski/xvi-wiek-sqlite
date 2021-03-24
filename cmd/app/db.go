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
							people TEXT,
							keywords TEXT,
							image TEXT,
							image_info TEXT
		);

		CREATE INDEX idx_facts_date ON facts(year, month, day);


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
				location, geo, people, keywords, image, image_info) 
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	stmtFact, err := tx.Prepare(sqlInsertFact)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtFact.Close()

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
				fact.Geo, fact.People, fact.Keywords, fact.Image, fact.ImageInfo)
			if err != nil {
				log.Fatal(err)
			}

			// id nowego rekordu w tabeli facts
			insertedId, err := result.LastInsertId()
			if err != nil {
				log.Fatal(err)
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
	var count int

	row := db.QueryRow("SELECT COUNT(*) FROM facts")
	err = row.Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	app.infoLog.Printf("Rekordów zapisanych w bazie: %d", count)
}
