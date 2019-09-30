// gqlmanager.go

package n3gql

import (
	"log"
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
		Types:   make(map[string]map[string]string, 100),
		Queries: make(map[string]string, 100),
	}
	execParams := &gqlengine.GraphQLParams{
		QueryRoot:        "n3query",
		SchemaDefinition: "",
		Resolvers:        buildResolvers(db),
	}

	// TODO: load any existing schema!!!

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

	schemaString, err := writeSchema(gqm.Schema)
	if err != nil {
		return errors.Wrap(err, "error writing gql schema:")
	}

	// store in the executor params so a new executor can be built on demand
	gqm.Lock()
	gqm.executorParams.SchemaDefinition = schemaString
	gqm.Unlock()

	// fmt.Printf("\n======= Schema =====\n\n%s\n\n", schemaString)

	return nil

}

//
// handler for gql requests
//
func (gqm *GQLManager) Query(query string, variables map[string]interface{}) (map[string]interface{}, error) {

	log.Println("...got query in gqlm()")
	// log.Printf("current schema:\n%s\n", gqm.executorParams.SchemaDefinition)

	executor, err := graphql.NewGraphQL(gqm.executorParams)
	if err != nil {
		return nil, errors.Wrap(err, "unable to construct executor gqlm.Query():")
	}

	context := map[string]interface{}{}
	return executor.Execute(context, query, variables, "")

}

//
// shut the schema-builder down gracefully, save schema
// for next restart of service.
//
func (gqm *GQLManager) Close() error {

	// TODO : persist schema here - context / gql
	// also reload (with comments)

	return nil

}
