// writeschema.go

package n3gql

import "strings"

//
// takes the aggregated schema data and creates a string
// in the required gql format for the whole schema
//
func writeSchema(sd SchemaData) (string, error) {

	var str strings.Builder

	// types
	for k, v := range sd.Types {
		str.WriteString("type ")
		str.WriteString(k)
		str.WriteString(" {\n")
		for k2, v2 := range v {
			str.WriteString("\t")
			str.WriteString(k2)
			str.WriteString(": ")
			str.WriteString(v2)
			str.WriteString("\n")
		}
		str.WriteString("}\n\n")
	}

	// query supprt for all known data
	str.WriteString("type n3data {\n")
	for k, v := range sd.Queries {
		str.WriteString("\t")
		str.WriteString(k)
		str.WriteString(": ")
		str.WriteString(v)
		str.WriteString("\n")
	}
	str.WriteString("}\n\n")

	// query root
	str.WriteString("type n3query {\n")
	str.WriteString("\tq(qspec: QueryInput!): n3data\n")
	str.WriteString("}\n\n")

	// query input type support
	// query enum
	str.WriteString("enum QueryType {\n")
	str.WriteString("\tfindById\n")
	str.WriteString("\tfindByType\n")
	str.WriteString("\tfindByValue\n")
	str.WriteString("\tfindByPredicate\n")
	str.WriteString("\ttraversalWithId\n")
	str.WriteString("\ttraversalWithValue\n")
	str.WriteString("}\n\n")

	// query filter input type
	str.WriteString("input FilterSpec {\n")
	str.WriteString("\teq: [String!]\n")
	str.WriteString("}\n\n")

	// query input type
	str.WriteString("input QueryInput {\n")
	str.WriteString("\tqueryType: QueryType!\n")
	str.WriteString("\tqueryValue: String!\n")
	str.WriteString("\ttraversal: [String!]\n")
	str.WriteString("\tfilters: [FilterSpec!]\n")
	str.WriteString("}\n\n")

	return str.String(), nil

}
