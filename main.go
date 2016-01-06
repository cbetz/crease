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

	c := session.DB("crease").C("users")

	updateDate := time.Now()

	// Skip the date line.
	scanner.Scan()

	// Skip the column header line.
	scanner.Scan()

	for scanner.Scan() {
		line := scanner.Text()

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

		data := Data{Score: score, WorkUnits: wu, Date: updateDate}
		user := User{Name: entry[0], Team: team}

		updateUserData(c, user, data)
	}

	if err := scanner.Err(); err != nil {
		println("error with scanner")
		log.Fatal(err)
	}
}

func updateUserData(c *mgo.Collection, user User, data Data) {
	result := new(User)
	err := c.Find(bson.M{"Name": user.Name}).One(&result)
	if err != nil {
		err = c.Insert(user)
		if err != nil {
			log.Fatal(err)
		}
		*result = user
	}

	c.Update(result, bson.M{"$push": bson.M{"data": data}})
}
