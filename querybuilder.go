// querybuilder.go

package n3gql

import (
	"context"
	"errors"
)

//
// receives SchemaData objects post-classification and determines
// types that need to be added to the root global query
// also assigns type to data structures with no root element
// such as XAPI
//
func queryBuilder(ctx context.Context, in <-chan SchemaData) (
	<-chan SchemaData, // emits SchemaData objects with gql types map
	<-chan error, // emits errors encountered to the pipeline manager
	error) { // any error encountered when creating this component

	out := make(chan SchemaData)
	errc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errc)
		for schemadata := range in { // read schema-data from upstream source

			sd := schemadata
			sd.Queries = make(map[string]string, 1)

			if sd.DataModel == sd.ClassifiedAs { // may indicate data has no root element, like xAPI
				// retrieve derived strucure from parsed type map
				structure, ok := sd.Types["n3-structure"]
				if !ok {
					errc <- errors.New("json object in queryBuilder() has no root or detected structure")
					return
				}
				// this is the definitive check..
				// if the strucure only has 1 entry, then the data is of that type
				// and is also of that data model, such as a Lesson.
				// Otherwise its a non-typed message, such as xAPI, so the derived
				// structure should be assigned for use in building the schema
				if len(structure) > 1 {
					sd.Types[sd.ClassifiedAs] = structure
				}
			}
			// remove the structure element, no longer needed
			delete(sd.Types, "n3-structure")

			// add the root type to the query map, always as an array of that type
			// to keep the unified query consistent
			sd.Queries[sd.ClassifiedAs] = "[" + sd.ClassifiedAs + "]"

			select {
			case out <- sd: // pass the data package on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}

		}
	}()

	return out, errc, nil

}
