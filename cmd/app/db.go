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

// wyszukuje rekord w podanej tabeli ze słowem kluczowym, nazwą o podanej
// wartości, zwraca id rekordu lub 0
func (app *application) findRec(tx *sql.Tx, nameOfIDField, table, name string) int64 {

	query := fmt.Sprintf("SELECT %s FROM %s WHERE name = '%s'", nameOfIDField, table, name)
	row, err := tx.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer row.Close()

	for row.Next() { // oczekiwany tylko jeden rekord
		var id int64
		row.Scan(&id)
		return id
	}

	return 0
}

// dodawanie nowego rekordu do tabeli people
func (app *application) addPerson(tx *sql.Tx, person string) int64 {
	var id int64

	stmtPeople, err := tx.Prepare(sqlInsertPeople)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtPeople.Close()

	result, err := stmtPeople.Exec(nil, person)
	if err != nil {
		log.Fatal(err)
	}

	id, err = result.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}

	return id
}

// dodawanie nowego rekordu do tabeli keywords
func (app *application) addKeyword(tx *sql.Tx, keyword string) int64 {
	var id int64

	stmtKeyword, err := tx.Prepare(sqlInsertKeyword)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtKeyword.Close()

	result, err := stmtKeyword.Exec(nil, keyword)
	if err != nil {
		log.Fatal(err)
	}

	id, err = result.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}

	return id
}

// dodawanie nowego rekordu do tabeli sources
func (app *application) addSource(tx *sql.Tx, fact_id int64, value, url_name, url string) {

	stmtSource, err := tx.Prepare(sqlInsertSource)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtSource.Close()

	_, err = stmtSource.Exec(nil, fact_id, value, url_name, url)
	if err != nil {
		log.Fatal(err)
	}
}

// dodawanie nowego rekordu do tabeli facts
func (app *application) addFact(tx *sql.Tx, number string, day, month, year int, title, content, contentTwitter, location, geo, image, imageInfo string) int64 {
	var insertedId int64

	stmtFact, err := tx.Prepare(sqlInsertFact)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtFact.Close()

	result, err := stmtFact.Exec(nil, number, day, month, year, title, content,
		contentTwitter, location, geo, image, imageInfo)
	if err != nil {
		log.Fatal(err)
	}

	insertedId, err = result.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}

	return insertedId
}

// funkcja przypisuje postać do wydarzenia historycznego
func (app *application) addFactPeople(tx *sql.Tx, factId, peopleId int64) {

	stmtFactPeople, err := tx.Prepare(sqlInsertFactPeople)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtFactPeople.Close()

	_, err = stmtFactPeople.Exec(factId, peopleId)
	if err != nil {
		log.Fatal(err)
	}

}

// funkcja przypisuje słowo kluczowe do wydarzenia historycznego
func (app *application) addFactKeyword(tx *sql.Tx, factId, keywordId int64) {

	stmtFactKeyword, err := tx.Prepare(sqlInsertFactKeyword)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtFactKeyword.Close()

	_, err = stmtFactKeyword.Exec(factId, keywordId)
	if err != nil {
		log.Fatal(err)
	}
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

	_, err = db.Exec(sqlCreateDb)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlCreateDb)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	for _, facts := range app.dataCache {
		for _, fact := range facts {

			// dołączenie nowego wydarzenia zwraca id nowego rekordu w tabeli facts
			insertedId := app.addFact(tx, fact.ID, fact.Day, fact.Month, fact.Year,
				fact.Title, fact.ContentText, fact.ContentTwitter, fact.Location,
				fact.Geo, fact.Image, fact.ImageInfo)

			// fact.People,
			if fact.People != "" {

				persons := strings.Split(fact.People, ";")
				for _, person := range persons {
					person = strings.TrimSpace(person)

					// pomijanie pustych wpisów
					if person == "" {
						continue
					}

					// weryfikacja czy już nie istnieje w bazie
					peopleInsertedId := app.findRec(tx, "people_id", "people", person)

					// jeżeli nie to dodaje nowy rekord do słownika people
					if peopleInsertedId == 0 {
						peopleInsertedId = app.addPerson(tx, person)
					}

					// podpięcie postaci do wydarzenia historycznego
					app.addFactPeople(tx, insertedId, peopleInsertedId)
				}
			}

			// fact.Keywords
			if fact.Keywords != "" {
				var keywordInsertedId int64

				keywords := strings.Split(fact.Keywords, ";")
				for _, keyword := range keywords {
					keyword = strings.TrimSpace(keyword)
					// pomijanie pustych wpisów
					if keyword == "" {
						continue
					}

					// weryfikacja czy już nie istnieje w bazie
					keywordInsertedId = app.findRec(tx, "keyword_id", "keywords", keyword)

					// jeżeli nie to dodaje nowy rekord do słownika keywords
					if keywordInsertedId == 0 {
						keywordInsertedId = app.addKeyword(tx, keyword)
					}

					// podpięcie słowa kluczowego do wydarzenia historycznego
					app.addFactKeyword(tx, insertedId, keywordInsertedId)
				}
			}

			// uzupełnienie bazy źródeł dla wydarzenia historycznego
			for _, source := range fact.Sources {
				app.addSource(tx, insertedId, source.Value, source.URLName, source.URL)
			}
		}
	}

	tx.Commit()

	// liczba dodanych rekordów dla poszczególnych tabel
	app.countRec(db, "facts")
	app.countRec(db, "sources")
	app.countRec(db, "people")
	app.countRec(db, "keywords")
	app.countRec(db, "fact_people")
	app.countRec(db, "fact_keywords")
}
