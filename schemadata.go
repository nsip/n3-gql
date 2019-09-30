// schemadata.go

package n3gql

//
// data package passed between stages of the
// schema-builder pipeline
//
//
type SchemaData struct {
	RawData      []byte
	ClassifiedAs string
	DataModel    string
	Types        map[string]map[string]string
	Queries      map[string]string
}
