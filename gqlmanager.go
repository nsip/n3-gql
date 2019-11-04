// gqlmanager.go

package n3gql

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	deep6 "github.com/nsip/n3-deep6"
	"github.com/pkg/errors"
	gqlengine "github.com/playlyfe/go-graphql"
	graphql "github.com/playlyfe/go-graphql"
)

type GQLManager struct {
	sync.Mutex
	Schema         SchemaData
	UserId         string
	ContextName    string
	executorParams *gqlengine.GraphQLParams
}

func NewGQLManager(userid, contextName string, db *deep6.Deep6DB) *GQLManager {
	// initialise internal structures
	sd := SchemaData{
		Types:   make(map[string]map[string]string),
		Queries: make(map[string]string),
	}
	//
	// load here from json if exists
	//
	contextPath := fmt.Sprintf("./contexts/%s/%s/gql", userid, contextName)
	schemaFile, err := os.Open(contextPath + "/schema.json")
	if err != nil {
		log.Println("...no existing schema found")
	} else {
		dec := json.NewDecoder(schemaFile)
		if err := dec.Decode(&sd); err != nil {
			log.Println("unabe to decode schema from file: ", err)
		} else {
			log.Println("...previous schema loaded from file.")
		}
	}

	execParams := &gqlengine.GraphQLParams{
		QueryRoot:        "n3query",
		SchemaDefinition: writeSchema(sd),
		Resolvers:        buildResolvers(db),
	}

	return &GQLManager{
		UserId:         userid,
		ContextName:    contextName,
		Schema:         sd,
		executorParams: execParams,
	}
}

//
// connects the schema builder to a channel providing json objects
// (typically the feed from the crdt manager)
// received objects will then be translated into gqphql-schema and fed to
// executor that will handle queries.
//
func (gqm *GQLManager) BuildSchemaFromJSONChannel(iterator <-chan []byte) error {

	err := runSchemaBuilder(gqm.UserId, gqm.ContextName, iterator, gqm.Schema)
	if err != nil {
		return err
	}

	// TODO - revist lock!!!
	// store in the executor params so a new executor can be built on demand
	gqm.Lock()
	schemaString := writeSchema(gqm.Schema)
	gqm.executorParams.SchemaDefinition = schemaString
	gqm.Unlock()

	// fmt.Printf("\n======= Schema =====\n\n%s\n\n", schemaString)

	return nil

}

//
// handler for gql requests
//
func (gqm *GQLManager) Query(query string, variables map[string]interface{}) (map[string]interface{}, error) {

	// TODO: revist lock!!!
	gqm.Lock()
	defer gqm.Unlock()

	params := gqm.executorParams

	// executor, err := graphql.NewGraphQL(gqm.executorParams)
	executor, err := graphql.NewGraphQL(params)
	if err != nil {
		return nil, errors.Wrap(err, "unable to construct executor gqlm.Query():")
	}

	context := map[string]interface{}{}
	results, err := executor.Execute(context, query, variables, "")
	if err != nil {
		return nil, err
	}

	return results, nil //executor.Execute(context, query, variables, "")

}

//
// shut the schema-builder down gracefully, save schema
// for next restart of service.
//
func (gqm *GQLManager) Close() error {

	// persist underlying schema data

	log.Println("...saving gql schema")
	s := gqm.Schema
	contextPath := fmt.Sprintf("./contexts/%s/%s/gql", gqm.UserId, gqm.ContextName)
	err := os.MkdirAll(contextPath, os.ModePerm)
	if err != nil {
		log.Println("unable to create schema folder: ", err)
		return err
	}
	f, err := os.Create(contextPath + "/schema.json")
	if err != nil {
		log.Println("unable to persist schema:", err)
		return err
	}

	enc := json.NewEncoder(f)
	if err := enc.Encode(&s); err != nil {
		log.Println("json encoding error: ", err)
		return err
	}

	log.Println("...schema saved.")

	return nil

}
