# gbdb
#### gbdb(go bed dataBase) a database inspiration from [dbdb(dog bed dataBase)](https://github.com/aosabook/500lines/tree/master/data-store)

### how to use?

`>go get github.com/Nuctori/gbdb/`  
to download the package

##### use example
````
package main

import (
	"fmt"
	"log"
	"github.com/Nuctori/gbdb"
)

func handleERR(err error) {
	if err != nil {
		log.Fatal("dbGet error:", err)
	}
}

func main() {
	db := gbdb.NewDB("mydb")
	db.Set("24", "student") // key and val can be int and string
	db.Set(25, "performer")
	job1, err := db.Get("24") // job1 is []bytes if no key in database return key error
	handleERR(err)
	job2, err := db.Get(25)
	handleERR(err)
	fmt.Println(string(job1)) // out:student
	fmt.Println(string(job2)) // out:performer
	db.Commit()               // data persistence
}

````

### LICENSE
Mit

### package interface
````
gbdb.NewDB()
gbdb.Get()
gbdb.Set()
gbdb.Pop()
````
