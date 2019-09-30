// jsonSource.go

package n3gql

import "context"

//
// source stage for pipeline, reads json objects
// from the provided channel and passes them onto
// next stages wrapped in data package
//
func jsonSource(ctx context.Context, iterator <-chan []byte) (
	<-chan SchemaData, // emits data packages for next stages
	<-chan error, // channel for reporting run-time errors
	error) { // any error encountered building component

	out := make(chan SchemaData)
	errc := make(chan error)

	go func() {
		defer close(out)
		defer close(errc)

		for json := range iterator {

			sdata := SchemaData{RawData: json}

			select {
			case out <- sdata: // pass the data package on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}
		}

	}()

	return out, errc, nil

}
