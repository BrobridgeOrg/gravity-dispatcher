package converter

import (
	"fmt"
	"reflect"

	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/BrobridgeOrg/schemer"
)

var (
	RecordTypes = map[schemer.ValueType]gravity_sdk_types_record.DataType{
		schemer.TYPE_NULL:    gravity_sdk_types_record.DataType_NULL,
		schemer.TYPE_INT64:   gravity_sdk_types_record.DataType_INT64,
		schemer.TYPE_UINT64:  gravity_sdk_types_record.DataType_UINT64,
		schemer.TYPE_FLOAT64: gravity_sdk_types_record.DataType_FLOAT64,
		schemer.TYPE_BOOLEAN: gravity_sdk_types_record.DataType_BOOLEAN,
		schemer.TYPE_STRING:  gravity_sdk_types_record.DataType_STRING,
		schemer.TYPE_BINARY:  gravity_sdk_types_record.DataType_BINARY,
		schemer.TYPE_TIME:    gravity_sdk_types_record.DataType_TIME,
		schemer.TYPE_ARRAY:   gravity_sdk_types_record.DataType_ARRAY,
		schemer.TYPE_MAP:     gravity_sdk_types_record.DataType_MAP,
	}
)

func getValue(t schemer.ValueType, data interface{}) (*gravity_sdk_types_record.Value, error) {
	return gravity_sdk_types_record.CreateValue(RecordTypes[t], data)
}

func convert(def *schemer.Definition, data interface{}) (*gravity_sdk_types_record.Value, error) {

	switch def.Type {
	case schemer.TYPE_ARRAY:

		v := reflect.ValueOf(data)

		// Prepare map value
		av := &gravity_sdk_types_record.ArrayValue{
			Elements: make([]*gravity_sdk_types_record.Value, 0, v.Len()),
		}

		for i := 0; i < v.Len(); i++ {
			ele := v.Index(i)

			// Convert value to protobuf format
			v, err := getValue(def.Subtype, ele.Interface())
			if err != nil {
				fmt.Println(err)
				continue
			}

			av.Elements = append(av.Elements, v)
		}

		return &gravity_sdk_types_record.Value{
			Type:  gravity_sdk_types_record.DataType_ARRAY,
			Array: av,
		}, nil

	case schemer.TYPE_MAP:

		v := reflect.ValueOf(data)

		if v.Kind() != reflect.Map {
			return nil, fmt.Errorf("Not a map object")
		}

		fields, err := Convert(def.Definition, data.(map[string]interface{}))
		if err != nil {
			return nil, err
		}

		// Prepare map value
		mv := &gravity_sdk_types_record.MapValue{
			Fields: fields,
		}

		return &gravity_sdk_types_record.Value{
			Type: gravity_sdk_types_record.DataType_MAP,
			Map:  mv,
		}, nil
	}

	return getValue(def.Type, data)
}

func Convert(schema *schemer.Schema, data map[string]interface{}) ([]*gravity_sdk_types_record.Field, error) {

	fields := make([]*gravity_sdk_types_record.Field, 0)

	if schema == nil {

		// No schema has been set so preparing record based on native types
		for fieldName, value := range data {
			v, err := gravity_sdk_types_record.GetValueFromInterface(value)
			if err != nil {
				fmt.Println(err)
				continue
			}

			field := &gravity_sdk_types_record.Field{
				Name:  fieldName,
				Value: v,
			}

			fields = append(fields, field)
		}

		return fields, nil
	}

	for fieldName, def := range schema.Fields {

		value, ok := data[fieldName]
		if !ok {
			continue
		}

		// Convert raw data
		v, err := convert(def, value)
		if err != nil {
			fmt.Println(err)
			continue
		}

		field := &gravity_sdk_types_record.Field{
			Name:  fieldName,
			Value: v,
		}

		fields = append(fields, field)
	}

	return fields, nil
}
