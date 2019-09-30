// typebuilder.go

package n3gql

import (
	"context"
	"math"

	"github.com/tidwall/gjson"
)

//
// receives SchemData objects post-classification and decomposes
// data into granular types for use in graphql schema generation
//
func typeBuilder(ctx context.Context, in <-chan SchemaData) (
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
			sd.Types = make(map[string]map[string]string)
			result := gjson.ParseBytes(sd.RawData)
			assignObject(sd.Types, "", result)

			select {
			case out <- sd: // pass the data package on to the next stage
			case <-ctx.Done(): // listen for pipeline shutdown
				return
			}

		}
	}()

	return out, errc, nil

}

//
// splits out the parent / child types from the json objects
// to create the necessary graphql schema definitions
//
func assignObject(tm map[string]map[string]string, parentKey string, val gjson.Result) {

	if parentKey == "" {
		parentKey = "n3-structure"
	}

	// see if we already have an entry for this outer key
	t, ok := tm[parentKey]
	if !ok {
		tm[parentKey] = make(map[string]string)
		t = tm[parentKey]
	}
	switch {
	case val.IsObject():
		val.ForEach(func(key, val gjson.Result) bool {
			switch {
			case val.IsObject():
				t[key.String()] = key.String()
				assignObject(tm, key.String(), val)
			case val.IsArray():
				arrayKey := key.String()
				t[arrayKey] = "[" + arrayKey + "]"
				val.ForEach(func(key, val gjson.Result) bool {
					if val.IsObject() {
						assignObject(tm, arrayKey, val)
					} else {
						t[arrayKey] = "[" + deriveScalarType(val) + "]"
					}
					return true
				})
			default:
				t[key.String()] = deriveScalarType(val)
			}
			return true
		})
	default:
		t[parentKey] = deriveScalarType(val)
	}

	tm[parentKey] = t

}

func deriveScalarType(val gjson.Result) string {

	switch val.Type {
	case gjson.Null:
		return "String" // best guess
	case gjson.False, gjson.True:
		return "Boolean"
	case gjson.Number:
		if val.Float() == math.Trunc(val.Float()) {
			return "Int"
		} else {
			return "Float"
		}
	case gjson.String:
		return "String"
	default:
		return "String"
	}

}
