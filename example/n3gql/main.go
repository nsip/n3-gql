// main.go

package main

import (
	"log"
	"time"

	n3context "github.com/nsip/n3-context"
)

//
// GQL Manager is hosted in a context, so we create one and send data
// through
//
func main() {

	// create a new manager
	cm1 := n3context.NewN3ContextManager()

	// add a context
	c1, err := cm1.AddContext("qglUser1", "gqlContext")
	if err != nil {
		log.Fatal(err)
	}

	// send in some data, via the crdt layer, sif which has object identifiers
	dataFile := "./sample_data/sif/sif.json"
	// dataFile := "./sample_data/xapi/xapi.json"
	err = c1.PublishFromFile(dataFile)
	if err != nil {
		log.Fatal("PublishFromFile() Error: ", err)
	}

	// dataFile := "./sample_data/sif/sif.json"
	// dataFile = "./sample_data/xapi/xapi.json"
	dataFile = "./sample_data/lessons/lessons.json"
	// dataFile = "./sample_data/subjects/subjects.json"
	err = c1.PublishFromFile(dataFile)
	if err != nil {
		log.Fatal("PublishFromFile() Error: ", err)
	}

	// // add another context
	// c2, err := cm1.AddContext("gqlUser2", "gqlContext")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// // send in some data, via the crdt layer from another user
	// // xapi to test handling of data without explicit object identifiers
	// dataFile = "./sample_data/xapi/xapi.json"
	// err = c2.PublishFromFile(dataFile)
	// if err != nil {
	// 	log.Fatal("PublishFromFile() Error: ", err)
	// }

	log.Println("...activating contexts")
	err = cm1.ActivateAll()
	if err != nil {
		log.Fatal(err)
	}

	// consume data for a time
	log.Println("...CM1 listening for updates")
	time.Sleep(time.Second * 10)
	// time.Sleep(time.Minute)

	// shut down the contexts, but persist details
	log.Println("Closing created contexts, and saving...")
	err = cm1.Close(true)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("...CM1 closed")

}
