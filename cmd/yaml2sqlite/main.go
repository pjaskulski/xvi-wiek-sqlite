package main

import (
	"flag"
	"log"
	"os"
	"path"

	"github.com/go-playground/validator"
)

// Source type
type Source struct {
	ID      string `yaml:"id"`
	Value   string `yaml:"value"`
	URLName string `yaml:"urlName"`
	URL     string `yaml:"url"`
}

// Fact type
type Fact struct {
	ID             string `yaml:"id" validate:"required"`
	Day            int    `yaml:"day" validate:"required"`
	Month          int    `yaml:"month" validate:"required"`
	Year           int    `yaml:"year" validate:"required"`
	Title          string `yaml:"title" validate:"required"`
	Content        string `yaml:"content" validate:"required"`
	ContentText    string
	ContentTwitter string   `yaml:"contentTwitter"`
	Location       string   `yaml:"location"`
	Geo            string   `yaml:"geo"`
	People         string   `yaml:"people"`
	Keywords       string   `yaml:"keywords"`
	Image          string   `yaml:"image"`
	ImageInfo      string   `yaml:"imageInfo"`
	Sources        []Source `yaml:"sources"`
}

type application struct {
	errorLog  *log.Logger
	infoLog   *log.Logger
	dataCache map[string][]Fact
}

var (
	numberOfFacts int
	dirExecutable string
)

// Config struct
type Config struct {
	DataPath   string
	OutputFile string
}

// Validate func
func (f *Fact) Validate() error {
	validate := validator.New()
	return validate.Struct(f)
}

func main() {
	// konfiguracja przez parametr z linii komend
	cfg := new(Config)
	flag.StringVar(&cfg.DataPath, "DataPath", "./data", "Ścieżka do katalogu z danymi, domyślnie ./data")
	flag.StringVar(&cfg.OutputFile, "OutputFile", "xvi-wiek.db", "Ścieżka do pliku z bazą danych, domyślnie xvi-wiek.db")
	flag.Parse()

	// ścieżka do pliku wykonywalnego
	ex, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}

	if isRunByRun() {
		dirExecutable = "."
	} else {
		dirExecutable = path.Dir(ex)
	}

	// logi z informacjami (->konsola) i błędami (->konsola)
	infoLog := log.New(os.Stdout, "INFO: \t", log.Ldate|log.Ltime)
	errorLog := log.New(os.Stdout, "ERROR: \t", log.Ldate|log.Ltime|log.Lshortfile)

	// aplikacja
	app := &application{
		errorLog:  errorLog,
		infoLog:   infoLog,
		dataCache: make(map[string][]Fact),
	}

	app.infoLog.Printf("yaml2sqlite - konwersja danych z plików yaml do bazy danych w formacie sqlite")

	_, err = os.Stat(cfg.DataPath)
	if os.IsNotExist(err) {
		app.errorLog.Fatalf("Folder z plikami yaml: %s nie istnieje.", cfg.DataPath)
	}

	// wczytane danych do pamięci podręcznej
	var path string
	if cfg.DataPath == "./data" {
		path = dirExecutable + cfg.DataPath
	} else {
		path = cfg.DataPath
	}
	err = app.loadData(path)
	if err != nil {
		app.errorLog.Fatal(err)
	}
	app.infoLog.Printf("Przetworzono %d wydarzeń", numberOfFacts)

	// główna funkcja konwertująca dane pobrane z plików yaml i tworząca plik
	// z bazą danych w formacje sqlite
	app.createSQLite(cfg.OutputFile)

	app.infoLog.Printf("yaml2sqlite - konwersja zakończona")
}
