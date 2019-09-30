// runschemabuilder.go

package n3gql

import (
	"context"

	"github.com/pkg/errors"
)

//
// builds & runs the pipeline for dynamically creating the gql schema based
// on data received
//
func runSchemaBuilder(userid string, contextName string, iterator <-chan []byte, schema SchemaData) error {

	// create context to manage pipeline
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// error channels to monitor pipeline
	var errcList []<-chan error

	// handle json input from crdt layer
	sourceOut, errc, err := jsonSource(ctx, iterator)
	if err != nil {
		return errors.Wrap(err, "unable to create schemaBuilder jsonSource() component")
	}
	errcList = append(errcList, errc)

	// classify the json
	classifierOut, errc, err := objectClassifier(ctx, userid, contextName, sourceOut)
	if err != nil {
		return errors.Wrap(err, "unable to create schemaBuilder objectClassifier() component:")
	}
	errcList = append(errcList, errc)

	// parse the data and build gql types map
	typeBuilderOut, errc, err := typeBuilder(ctx, classifierOut)
	if err != nil {
		return errors.Wrap(err, "unable to create schemaBuilder typeBuilder() component:")
	}
	errcList = append(errcList, errc)

	// determine root & query types
	queryBuilderOut, errc, err := queryBuilder(ctx, typeBuilderOut)
	if err != nil {
		return errors.Wrap(err, "unable to create schemaBuilder queryBuilder() component:")
	}
	errcList = append(errcList, errc)

	// merge into full schema
	mergeOut, errc, err := mergeSchema(ctx, schema, queryBuilderOut)
	if err != nil {
		return errors.Wrap(err, "unable to create schemaBuilder mergeSchema() component:")
	}
	errcList = append(errcList, errc)

	// audit sink
	go func() {
		for sd := range mergeOut {
			// fmt.Println("==========")
			// fmt.Printf("schema-builder (%s|%s) received:%s:%s\n", userid, contextName, sd.DataModel, sd.ClassifiedAs)
			// for k, v := range sd.Types {
			// 	fmt.Printf("\nkey:%s\n%v\n", k, v)
			// }
			// for k, v := range sd.Queries {
			// 	fmt.Printf("\nquery:%s : %s\n\n", k, v)
			// }
			// fmt.Println("==========")
			_ = sd
		}
	}()

	return WaitForPipeline(errcList...)

}
