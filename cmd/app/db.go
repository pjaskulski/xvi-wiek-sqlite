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
func (app *application) countRec(tableName string) {
	var count int

	row := app.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	err := row.Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	app.infoLog.Printf("Rekordów zapisanych w tabeli '%s': %d", tableName, count)
}

// wyszukuje rekord w podanej tabeli ze słowem kluczowym, nazwą o podanej
// wartości, zwraca id rekordu lub 0
func (app *application) findRec(nameOfIDField, table, name string) int64 {

	query := fmt.Sprintf("SELECT %s FROM %s WHERE name = '%s'", nameOfIDField, table, name)
	row, err := app.tx.Query(query)
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
func (app *application) addPerson(person string) int64 {
	var id int64

	stmtPeople, err := app.tx.Prepare(sqlInsertPeople)
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

// dodawanie nowego rekordu do tabeli locations
func (app *application) addLocation(location, geo string) int64 {
	var id int64

	stmtLocation, err := app.tx.Prepare(sqlInsertLocation)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtLocation.Close()

	result, err := stmtLocation.Exec(nil, location, geo)
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
func (app *application) addKeyword(keyword string) int64 {
	var id int64

	stmtKeyword, err := app.tx.Prepare(sqlInsertKeyword)
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
func (app *application) addSource(fact_id int64, value, url_name, url string) {

	stmtSource, err := app.tx.Prepare(sqlInsertSource)
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
func (app *application) addFact(number string, day, month, year int, title, content, contentTwitter, image, imageInfo string) int64 {
	var insertedId int64

	stmtFact, err := app.tx.Prepare(sqlInsertFact)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtFact.Close()

	factDate := fmt.Sprintf("%04d-%02d-%02d", year, month, day)

	result, err := stmtFact.Exec(nil, number, factDate, day, month, year, title, content,
		contentTwitter, image, imageInfo)
	if err != nil {
		log.Fatal(err)
	}

	insertedId, err = result.LastInsertId()
	if err != nil {
		log.Fatal(err)
	}

	return insertedId
}

// uzupełnienie rekordu tabeli facts o lokalizację i pozycję geograficzną
func (app *application) updateFact(fact_id, location_id int64) {

	stmtUpdateFact, err := app.tx.Prepare(sqlUpdateFact)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtUpdateFact.Close()

	_, err = stmtUpdateFact.Exec(location_id, fact_id)
	if err != nil {
		log.Fatal(err)
	}
}

// funkcja przypisuje postać do wydarzenia historycznego
func (app *application) addFactPeople(factId, peopleId int64) {

	stmtFactPeople, err := app.tx.Prepare(sqlInsertFactPeople)
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
func (app *application) addFactKeyword(factId, keywordId int64) {

	stmtFactKeyword, err := app.tx.Prepare(sqlInsertFactKeyword)
	if err != nil {
		log.Fatal(err)
	}
	defer stmtFactKeyword.Close()

	_, err = stmtFactKeyword.Exec(factId, keywordId)
	if err != nil {
		log.Fatal(err)
	}
}

// raport z liczbą dodanych rekordów
func (app *application) countReport() {
	app.countRec("facts")
	app.countRec("sources")
	app.countRec("people")
	app.countRec("keywords")
	app.countRec("locations")
	app.countRec("fact_people")
	app.countRec("fact_keywords")
}

// funkcja tworzy bazę danych w formacie sqlite i wypełnia danymi pobranymi
// wcześniej z plików yaml
func (app *application) createSQLite(filename string) {
	var err error

	if fileExists(filename) {
		os.Remove(filename)
	}

	app.db, err = sql.Open("sqlite3", fmt.Sprintf("file:%s?_foreign_keys=on", filename))
	if err != nil {
		log.Fatal(err)
	}
	defer app.db.Close()

	_, err = app.db.Exec(sqlCreateDb)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlCreateDb)
	}

	app.tx, err = app.db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	for _, facts := range app.dataCache {
		for _, fact := range facts {

			// dołączenie nowego wydarzenia zwraca id nowego rekordu w tabeli facts
			insertedId := app.addFact(fact.ID, fact.Day, fact.Month, fact.Year,
				fact.Title, fact.ContentText, fact.ContentTwitter, fact.Image, fact.ImageInfo)

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
					peopleInsertedId := app.findRec("people_id", "people", person)

					// jeżeli nie to dodaje nowy rekord do słownika people
					if peopleInsertedId == 0 {
						peopleInsertedId = app.addPerson(person)
					}

					// podpięcie postaci do wydarzenia historycznego
					app.addFactPeople(insertedId, peopleInsertedId)
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
					keywordInsertedId = app.findRec("keyword_id", "keywords", keyword)

					// jeżeli nie to dodaje nowy rekord do słownika keywords
					if keywordInsertedId == 0 {
						keywordInsertedId = app.addKeyword(keyword)
					}

					// podpięcie słowa kluczowego do wydarzenia historycznego
					app.addFactKeyword(insertedId, keywordInsertedId)
				}
			}

			// fact.Location,
			location := strings.TrimSpace(fact.Location)
			geo := strings.TrimSpace(fact.Geo)
			if location != "" {
				// weryfikacja czy już nie istnieje w bazie
				locationInsertedId := app.findRec("location_id", "locations", location)

				// jeżeli nie to dodaje nowy rekord do słownika locations
				if locationInsertedId == 0 {
					locationInsertedId = app.addLocation(location, geo)
				}

				// podpięcie lokalizacji do wydarzenia historycznego
				app.updateFact(insertedId, locationInsertedId)
			}

			// uzupełnienie bazy źródeł dla wydarzenia historycznego
			for _, source := range fact.Sources {
				app.addSource(insertedId, source.Value, source.URLName, source.URL)
			}
		}
	}

	app.tx.Commit()

	// liczba dodanych rekordów dla poszczególnych tabel
	app.countReport()
}
