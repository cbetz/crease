package main

import (
	"bufio"
	"database/sql"
	"log"
	"os"
	"strings"
	"time"

	"github.com/lib/pq"
)

type User struct {
	Name string
	Team uint64
	Data []Data
}

type Data struct {
	Score     uint64
	WorkUnits uint64
	Date      time.Time
}

func main() {

	/*output, err := os.Create("daily_user_summary.txt.bz2")
	if err != nil {
		log.Fatal(err)
	}
	defer output.Close()

	response, err := http.Get("http://fah-web.stanford.edu/daily_user_summary.txt.bz2")
	if err != nil {
		log.Fatal(err)
	}
	defer response.Body.Close()

	n, err := io.Copy(output, response.Body)
	if err != nil {
		log.Fatal(err)
	}
	print(n, " bytes downloaded\n")

	compressed, err := os.Open("daily_user_summary.txt.bz2")
	if err != nil {
		log.Fatal(err)
	}
	defer compressed.Close()

	uncompressed := bzip2.NewReader(compressed)

	var e = charmap.Windows1252
	decoded := transform.NewReader(uncompressed, e.NewDecoder())

	buf, err := ioutil.ReadAll(decoded)
	if err != nil {
		log.Fatal(err)
	}

	err = ioutil.WriteFile("daily_user_summary.txt", buf, 0666)
	if err != nil {
		println("error writing file: ")
		log.Fatal(err)
	} */

	file, err := os.Open("daily_user_summary.txt")
	if err != nil {
		println("error opening file: ")
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	// Skip the date line.
	scanner.Scan()

	// Skip the column header line.
	scanner.Scan()

	start := time.Now()

	// lazily open db (doesn't truly open until first request)
	db, err := sql.Open("postgres", "host=localhost dbname=crease sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("users", "name"))
	if err != nil {
		log.Fatal(err)
	}

	for scanner.Scan() {
		entry := strings.Split(scanner.Text(), "\t")

		user := User{Name: entry[0]}

		_, err = stmt.Exec(user.Name)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)
	log.Printf("Processed in %s", elapsed)
}
