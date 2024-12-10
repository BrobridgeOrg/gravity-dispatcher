package converter

import (
	"encoding/base64"
	"fmt"
	"reflect"

	record_type "github.com/BrobridgeOrg/gravity-sdk/v2/types/record"
	"github.com/BrobridgeOrg/schemer"
)

var (
	RecordTypes = map[schemer.ValueType]record_type.DataType{
		schemer.TYPE_NULL:    record_type.DataType_NULL,
		schemer.TYPE_INT64:   record_type.DataType_INT64,
		schemer.TYPE_UINT64:  record_type.DataType_UINT64,
		schemer.TYPE_FLOAT64: record_type.DataType_FLOAT64,
		schemer.TYPE_BOOLEAN: record_type.DataType_BOOLEAN,
		schemer.TYPE_STRING:  record_type.DataType_STRING,
		schemer.TYPE_BINARY:  record_type.DataType_BINARY,
		schemer.TYPE_TIME:    record_type.DataType_TIME,
		schemer.TYPE_ARRAY:   record_type.DataType_ARRAY,
		schemer.TYPE_MAP:     record_type.DataType_MAP,
	}
)

func getValue(t schemer.ValueType, data interface{}) (*record_type.Value, error) {

	if t == schemer.TYPE_BINARY {
		if dataBytes, ok := data.([]uint8); ok {
			// Convert base64 (from json) string to binary
			bytes, err := base64.StdEncoding.DecodeString(string(dataBytes))
			if err != nil {
				return nil, err
			}

			return record_type.CreateValue(RecordTypes[t], bytes)
		}
	}

	return record_type.CreateValue(RecordTypes[t], data)
}

func convert(def *schemer.Definition, data interface{}) (*record_type.Value, error) {

	switch def.Type {
	case schemer.TYPE_ARRAY:

		v := reflect.ValueOf(data)

		// Prepare map value
		av := &record_type.ArrayValue{
			Elements: make([]*record_type.Value, 0, v.Len()),
		}

		for i := 0; i < v.Len(); i++ {
			ele := v.Index(i)

			// Convert value to protobuf format
			v, err := getValue(def.Subtype.Type, ele.Interface())
			if err != nil {
				fmt.Println(err)
				continue
			}

			av.Elements = append(av.Elements, v)
		}

		return &record_type.Value{
			Type:  record_type.DataType_ARRAY,
			Array: av,
		}, nil

	case schemer.TYPE_MAP:

		v, ok := data.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Not a map object")
		}

		fields, err := convertMap(def.Schema, v, false)
		if err != nil {
			return nil, err
		}

		// Prepare map value
		mv := &record_type.MapValue{
			Fields: fields,
		}

		return &record_type.Value{
			Type: record_type.DataType_MAP,
			Map:  mv,
		}, nil
	}

	return getValue(def.Type, data)
}

func convertMap(schema *schemer.Schema, data map[string]interface{}, isRoot bool) ([]*record_type.Field, error) {

	fields := make([]*record_type.Field, 0)

	if schema == nil {

		// No schema has been set so preparing record based on native types
		for fieldName, value := range data {
			v, err := record_type.GetValueFromInterface(value)
			if err != nil {
				fmt.Println(err)
				continue
			}

			field := &record_type.Field{
				Name:  fieldName,
				Value: v,
			}

			fields = append(fields, field)
		}

		return fields, nil
	}

	for k, v := range data {

		if isRoot && k == "$removedFields" {

			switch d := v.(type) {
			case []interface{}:

				elements := make([]*record_type.Value, 0, len(d))

				for _, fieldName := range d {
					name, ok := fieldName.(string)
					if !ok {
						continue
					}

					v, err := record_type.CreateValue(record_type.DataType_STRING, name)
					if err != nil {
						fmt.Println(err)
						continue
					}

					elements = append(elements, v)
				}

				field := &record_type.Field{
					Name: "$removedFields",
					Value: &record_type.Value{
						Type: record_type.DataType_ARRAY,
						Array: &record_type.ArrayValue{
							Elements: elements,
						},
					},
				}
				fields = append(fields, field)
			}

			continue
		}

		def := schema.GetDefinition(k)
		if def == nil {
			fmt.Println("Definition not found for field", k)
		}

		// Convert raw data
		v, err := convert(def, v)
		if err != nil {
			fmt.Println(err)
			continue
		}

		field := &record_type.Field{
			Name:  k,
			Value: v,
		}

		fields = append(fields, field)
	}

	return fields, nil
}

func Convert(schema *schemer.Schema, data map[string]interface{}) ([]*record_type.Field, error) {
	return convertMap(schema, data, true)
}
