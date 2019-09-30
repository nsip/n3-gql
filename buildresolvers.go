// buildresolvers.go

package n3gql

import (
	"errors"
	"time"

	deep6 "github.com/nsip/n3-deep6"
	graphql "github.com/playlyfe/go-graphql"
)

//
// builds the generic resolver function for all gql queries
//
func buildResolvers(db *deep6.Deep6DB) map[string]interface{} {

	m := map[string]interface{}{}

	//
	// the universal handler function
	//
	f := func(params *graphql.ResolveParams) (interface{}, error) {

		timeTrack(time.Now(), "n3DataQuery() ")
		// log.Println("...default resolver called")

		//
		// get query variables from params
		// type : value : traversal : filters etc.
		//
		input, ok := params.Args["qspec"].(map[string]interface{})
		if !ok {
			return nil, errors.New("no query input object provided.")
		}
		// log.Printf("input:\n%v\n\n", input)

		qtype, ok := input["queryType"].(string)
		if !ok {
			return nil, errors.New("query type not specified, is required")
		}
		qval, ok := input["queryValue"].(string)
		if !ok {
			return nil, errors.New("queryValue must be set")
		}
		//
		// re-structure the traversal spec if provided
		// required becasue playlyfe lib only passes interface{} objects
		// and we need to assert types in golang traversal spec.
		//
		tSpec := deep6.Traversal{TraversalSpec: make([]string, 0)}
		if ts, ok := input["traversal"]; ok {
			for _, token := range ts.([]interface{}) {
				tSpec.TraversalSpec = append(tSpec.TraversalSpec, token.(string))
			}
		}
		//
		// re-structure the filters if provided
		// to save gql users typing/noise filters are expressed as
		// (operation e.g 'eq':) followed by triplet of strings
		// [DataType | Predicate | TargetValue] which are here
		// unpacked from the interfaces{} playlyfe provides as
		// query params back into the required golang structs
		//
		fSpec := deep6.FilterSpec{}
		if filters, ok := input["filters"]; ok {
			for _, filter := range filters.([]interface{}) {
				for _, v := range filter.(map[string]interface{}) {
					triple := make([]string, 0, 3)
					for _, x := range v.([]interface{}) {
						triple = append(triple, x.(string))
					}
					if len(triple) < 3 {
						return nil, errors.New("Filter must contain 3 elements.")
					}
					f := deep6.Filter{Predicate: triple[1], TargetValue: triple[2]}
					fSpec[triple[0]] = append(fSpec[triple[0]], f)
				}
			}
		}

		// log.Printf("query params:\nquery-type:%s\nquery-value:%s\ntraversal:%v\nfilters:%v\n\n", qtype, qval, tSpec, fSpec)

		var results map[string][]map[string]interface{}
		var dbErr error

		switch qtype {
		case "findById":
			results, dbErr = db.FindById(qval)
		case "findByType":
			results, dbErr = db.FindByType(qval, fSpec)
		case "findByValue":
			results, dbErr = db.FindByValue(qval, fSpec)
		case "findByPredicate":
			results, dbErr = db.FindByPredicate(qval, fSpec)
		case "traversalWithId":
			results, dbErr = db.TraversalWithId(qval, tSpec, fSpec)
		case "traversalWithValue":
			results, dbErr = db.TraversalWithValue(qval, tSpec, fSpec)
		}
		if dbErr != nil {
			return nil, dbErr
		}

		// log.Printf("raw-results:\n%s\n\n", results)
		gqlResults := gqlTransform(results)
		// log.Printf("transformed-results:\n%s\n\n", gqlResults)

		return gqlResults, nil

	}

	//
	// assign to the query root's only method
	//
	m["n3query/q"] = f
	return m

}

//
// strip verbose results keys not needed for gql response
//
func gqlTransform(results map[string][]map[string]interface{}) map[string]interface{} {

	transformedResults := make(map[string]interface{}, 0)

	for resultType, resultsArray := range results {
		objectArray := make([]map[string]interface{}, 0)
		for _, v := range resultsArray {
			if objectContent, ok := v[resultType].(map[string]interface{}); ok {
				objectArray = append(objectArray, objectContent)
			} else {
				objectArray = append(objectArray, v)
			}
		}
		transformedResults[resultType] = objectArray
	}

	return transformedResults

}
