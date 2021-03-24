package main

import (
	"os"
	"path"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

func filenameWithoutExtension(fn string) string {
	return strings.TrimSuffix(fn, path.Ext(fn))
}

// isRunByRun - funkcja sprawdza czy uruchomiono program przez go run
// czy też program skompilowany, funkcja dla systemu Linux
func isRunByRun() bool {
	return strings.Contains(os.Args[0], "/tmp/go-build")
}

// czy plik istnieje
func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

// funkcja usuwa style (kapitaliki, italiki, pogrubienia) z danych pobranych w pliku yaml
func prepareTextStyle(content string, clear bool) string {

	content = strings.Replace(content, `{`, ``, -1)
	content = strings.Replace(content, `}`, ``, -1)

	return content
}
