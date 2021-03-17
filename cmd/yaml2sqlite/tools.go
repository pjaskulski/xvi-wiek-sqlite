package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

func filenameWithoutExtension(fn string) string {
	return strings.TrimSuffix(fn, path.Ext(fn))
}

// isRunByRun - funkcja sprawdza czy uruchomiono program przez go run
// czy też program skompilowany, funkcja dla systemu Linux
func isRunByRun() bool {
	return strings.Contains(os.Args[0], "/tmp/go-build")
}

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

// funkcja usuwa style (kapitaliki, italiki, pogrubienia) z danych pobranych w pliku yaml
func prepareTextStyle(content string, clear bool) string {

	content = strings.Replace(content, `{`, ``, -1)
	content = strings.Replace(content, `}`, ``, -1)

	return content
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

func (app *application) createSQLite(filename string) {

	fmt.Println("Create SQLite")
}
