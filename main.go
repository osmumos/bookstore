/*
** Tutorial on how to Access DB in GO using database/sql package
 */

package main

import (
	//Alias the Package Name to the Blank Identifier (_) so that its pq.init() is called to register itself with database/sql
	//But we cannot use it directly
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"

	_ "github.com/lib/pq"
)

//Create the Book type with struct
//If the DB allowed NULLs then use sql.NullString; sql.NullFloat64 etc
type Book struct {
	isbn   string
	title  string
	author string
	price  float32
}

//A global variable to hold the db connection
var db *sql.DB

func init() {
	var err error

	//Initialize a new sql.DB object by calling sql.Open(). pass in name of the driver
	//You can change the Pool size using the returned db object e.g. db.SetMaxOpenConns() and db.SetMaxIdleConns()

	db, err = sql.Open("postgres", "postgres://postgres:paradigmAdm!n@35.194.20.123:5432/bookstore")
	if err != nil {
		log.Fatal(err)
	}

	//Check the Connection using db.Ping() because sql.Open() doesn't check whether the connection is open
	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	//Start the HTTP Server
	http.HandleFunc("/books", booksIndex)
	http.HandleFunc("/books/show", booksShow)
	http.HandleFunc("/books/create", booksCreate)
	http.ListenAndServe(":3000", nil)
}

func booksIndex(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {

		//Return a Method Not Allowed for any non-GET request
		http.Error(w, http.StatusText(405), 405)
		return
	}

	//Fetch a resultset and assign to a rows variable
	rows, err := db.Query("SELECT * FROM books")
	if err != nil {
		log.Fatal(err)
	}

	/*
	** Important: Ensure resultset is closed properly before parent function returns.
	** This ensures no hanging connections. The pool remains healthy.append
	** Call defer after first checking for errors. Avoids getting a Panic when trying to close a nil resultset.
	 */
	defer rows.Close()

	bks := make([]*Book, 0)

	//After reaching EOF, the resultset automatically closes itself and releases the connection back to the pool.
	for rows.Next() {
		bk := new(Book)

		//Copy data from all the fields using scan into the bk object. Check for errors
		err := rows.Scan(&bk.isbn, &bk.title, &bk.author, &bk.price)
		if err != nil {
			log.Fatal(err)
		}

		//Add the new book to the books slice i.e. collection
		bks = append(bks, bk)
	}

	//Check for any errors that might have occured during the interaction
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	//Now loop through the populated bks slice
	for _, bk := range bks {
		fmt.Fprintf(w, "%s, %s, %s, £%.2f\n", bk.isbn, bk.title, bk.author, bk.price)
	}
}

//Querying a single row
func booksShow(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, http.StatusText(405), 405)
		return
	}

	//Get the querystring parameter. returns empty string if none was found
	//Hence check for empty string and return Bad Request
	isbn := r.FormValue("isbn")
	if isbn == "" {
		http.Error(w, http.StatusText(400), 400)
		return
	}

	// Use Placeholder Parameters. Postgres uses $x while MySQL and MSSQL use ?
	//Works for db.Query(), db.QueryRow() and db.Exec() to avoid SQL-Injection
	row := db.QueryRow("SELECT * FROM books WHERE isbn = $1", isbn)

	bk := new(Book)

	//If no rows were returned, the error will be thrown by row.Scan()
	err := row.Scan(&bk.isbn, &bk.title, &bk.author, &bk.price)
	if err == sql.ErrNoRows {
		http.NotFound(w, r)
		return
	} else if err != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}

	fmt.Fprintf(w, "%s, %s, %s, £%.2f\n", bk.isbn, bk.title, bk.author, bk.price)
}

//Create a New Book
//e.g. curl -i -X POST -d "isbn=978-1470184841&title=Metamorphosis&author=Franz Kafka&price=5.90" localhost:3000/books/create
func booksCreate(w http.ResponseWriter, r *http.Request) {

	//Ensure only POST method is allowed
	if r.Method != "POST" {
		http.Error(w, http.StatusText(405), 405)
		return
	}

	//Get the Form Parameters
	isbn := r.FormValue("isbn")
	title := r.FormValue("title")
	author := r.FormValue("author")
	if isbn == "" || title == "" || author == "" {
		http.Error(w, http.StatusText(400), 400)
		return
	}

	//Parse string for price
	price, err := strconv.ParseFloat(r.FormValue("price"), 32)
	if err != nil {
		http.Error(w, http.StatusText(400), 400)
		return
	}

	// Use EXEC for Queries that don't return rows
	// DB.Exec(), like DB.Query() and DB.QueryRow(), is a variadic function, which means you can pass in as many parameters as you need.
	result, err := db.Exec("INSERT INTO books VALUES($1, $2, $3, $4)", isbn, title, author, price)
	if err != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}

	//If you don't want to use the sql.Result object you can discard it using a blank identifier
	//The sql.Result() interface exposes LastInsertedId() (not supported by PQ) and RowsAffected()
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		http.Error(w, http.StatusText(500), 500)
		return
	}

	//In Postgresql to return the LastInsertId() use:
	/*
		var id int
		err := db.QueryRow("INSERT INTO user (name) VALUES ('John') RETURNING id").Scan(&id)
		if err != nil {
			...
		}
	*/

	fmt.Fprintf(w, "Book %s created successfully (%d row affected)\n", isbn, rowsAffected)
}
