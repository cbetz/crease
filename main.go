package main

import (
	"bufio"
	"compress/bzip2"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
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

	output, err := os.Create("daily_user_summary.txt.bz2")
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
	}

	file, err := os.Open("daily_user_summary.txt")
	if err != nil {
		println("error opening file: ")
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	session, err := mgo.Dial("localhost")
	if err != nil {
		println("error dialing DB")
		log.Fatal(err)
	}
	defer session.Close()

	// Skip the date line.
	scanner.Scan()

	// Skip the column header line.
	scanner.Scan()

	for {
		// Create a wait group to manage the goroutines.
		var waitGroup sync.WaitGroup

		start := time.Now()

		// Perform 10 concurrent queries against the database.
		waitGroup.Add(10)
		for query := 0; query < 10; query++ {
			if scanner.Scan() {
				go updateUserData(query, &waitGroup, session, scanner.Text())
			} else {
				break
			}
		}

		// Wait for all the queries to complete.
		waitGroup.Wait()

		elapsed := time.Since(start)
		log.Printf("Processed in %s", elapsed)

		if scanner.Scan() == false {
			break
		}
	}
}

func updateUserData(query int, waitGroup *sync.WaitGroup, mongoSession *mgo.Session, line string) {
	entry := strings.Split(line, "\t")

	score, err := strconv.ParseUint(entry[1], 10, 64)
	if err != nil {
		println("error parsing score")
	}

	wu, err := strconv.ParseUint(entry[2], 10, 64)
	if err != nil {
		println("error parsing wu")
	}

	team, err := strconv.ParseUint(entry[3], 10, 64)
	if err != nil {
		println("error parsing team:" + err.Error())
	}

	updateDate := time.Now()
	data := Data{Score: score, WorkUnits: wu, Date: updateDate}
	user := User{Name: entry[0], Team: team}

	// Decrement the wait group count so the program knows this
	// has been completed once the goroutine exits.
	defer waitGroup.Done()

	// Request a socket connection from the session to process our query.
	// Close the session when the goroutine exits and put the connection back
	// into the pool.
	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()

	c := sessionCopy.DB("crease").C("users")

	result := new(User)
	err = c.Find(bson.M{"name": user.Name}).One(&result)
	if err != nil {
		err = c.Insert(user)
		if err != nil {
			log.Fatal(err)
		}
		*result = user
	}

	c.Update(result, bson.M{"$push": bson.M{"data": data}})
}
