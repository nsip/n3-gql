// mergeschema.go

package n3gql

import (
	"context"
)

//
// receives SchemaData objects for each parsed json object, and updates
// the gqlManager's overall schema.
//
func mergeSchema(ctx context.Context, schema SchemaData, in <-chan SchemaData) (
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

			// aggregate queries
			for k, v := range sd.Queries {
				schema.Queries[k] = v
			}

			// aggregate types
			for k, v := range sd.Types {
				_, ok := schema.Types[k]
				if !ok { // add if not previously specified
					schema.Types[k] = v
				} else { // otherwise aggregate
					for k2, v2 := range v {
						schema.Types[k][k2] = v2
					}
				}
			}

			select {
			case out <- sd: // pass the data package on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}

		}
	}()

	return out, errc, nil

}
