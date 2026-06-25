package jq

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/itchyny/gojq"
	"github.com/onsi/gomega/format"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type TransformFn func(obj *unstructured.Unstructured) error

func TransformPipeline(steps ...TransformFn) TransformFn {
	return func(obj *unstructured.Unstructured) error {
		for _, step := range steps {
			if err := step(obj); err != nil {
				return err
			}
		}

		return nil
	}
}

func Transform(format string, args ...any) TransformFn {
	expression := fmt.Sprintf(format, args...)

	return func(in *unstructured.Unstructured) error {
		query, err := gojq.Parse(expression)
		if err != nil {
			return fmt.Errorf("unable to parse expression %q: %w", expression, err)
		}

		result, ok := query.Run(in.Object).Next()
		if !ok || result == nil {
			return nil
		}

		if err, ok := result.(error); ok {
			return fmt.Errorf("query execution error: %w", err)
		}

		uc, ok := result.(map[string]any)
		if !ok {
			return fmt.Errorf("expected map[string]interface{}, got %T", result)
		}

		in.SetUnstructuredContent(uc)

		return nil
	}
}

func ExtractValue[T any](in any, expression string) (T, error) {
	var result T

	query, err := gojq.Parse(expression)
	if err != nil {
		return result, fmt.Errorf("unable to parse expression %s, %w", expression, err)
	}

	data, err := toType(in)
	if err != nil {
		return result, err
	}

	it := query.Run(data)

	v, ok := it.Next()
	if !ok {
		return result, nil
	}

	if err, ok := v.(error); ok {
		return result, err
	}

	result, ok = v.(T)
	if !ok {
		rv := reflect.ValueOf(v)
		rt := reflect.TypeFor[T]()

		if rv.CanConvert(rt) {
			result, _ = rv.Convert(rt).Interface().(T)
		} else {
			return result, fmt.Errorf("result value is not of the expected type (expected:%T, got:%T", result, v)
		}
	}

	return result, nil
}

func formattedMessage(comparisonMessage string, failurePath []any) string {
	diffMessage := ""

	if len(failurePath) != 0 {
		diffMessage = "\n\nfirst mismatched key: " + formattedFailurePath(failurePath)
	}

	return comparisonMessage + diffMessage
}

func formattedFailurePath(failurePath []any) string {
	formattedPaths := make([]string, 0)

	for i := len(failurePath) - 1; i >= 0; i-- {
		switch p := failurePath[i].(type) {
		case int:
			val := fmt.Sprintf(`[%d]`, p)
			formattedPaths = append(formattedPaths, val)
		default:
			if i != len(failurePath)-1 {
				formattedPaths = append(formattedPaths, ".")
			}

			val := fmt.Sprintf(`"%s"`, p)
			formattedPaths = append(formattedPaths, val)
		}
	}

	return strings.Join(formattedPaths, "")
}

//nolint:cyclop
func toType(in any) (any, error) {
	valof := reflect.ValueOf(in)
	if !valof.IsValid() {
		return nil, nil
	}
	if valof.Kind() == reflect.Ptr && valof.IsNil() {
		return nil, nil
	}

	switch v := in.(type) {
	case string:
		return byteToType([]byte(v))
	case []byte:
		return byteToType(v)
	case json.RawMessage:
		return byteToType(v)
	case io.Reader:
		data, err := io.ReadAll(v)
		if err != nil {
			return nil, fmt.Errorf("failed to read from reader: %w", err)
		}

		return byteToType(data)
	case unstructured.UnstructuredList:
		res := make([]any, 0, len(v.Items))
		for i := range v.Items {
			d, err := normalizeObject(v.Items[i].Object)
			if err != nil {
				return nil, err
			}
			res = append(res, d)
		}
		return res, nil
	case []unstructured.Unstructured:
		res := make([]any, 0, len(v))
		for i := range v {
			d, err := normalizeObject(v[i].Object)
			if err != nil {
				return nil, err
			}
			res = append(res, d)
		}
		return res, nil
	case unstructured.Unstructured:
		return normalizeObject(v.Object)
	case []*unstructured.Unstructured:
		res := make([]any, 0, len(v))
		for i := range v {
			if v[i] == nil {
				return nil, fmt.Errorf("nil pointer at index %d in []*unstructured.Unstructured", i)
			}
			d, err := normalizeObject(v[i].Object)
			if err != nil {
				return nil, err
			}
			res = append(res, d)
		}
		return res, nil
	case *unstructured.Unstructured:
		return normalizeObject(v.Object)
	}

	switch reflect.TypeOf(in).Kind() {
	case reflect.Map, reflect.Slice:
		data, err := json.Marshal(in)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal object: %w", err)
		}

		return byteToType(data)
	default:
		return nil, fmt.Errorf("unsuported type:\n%s", format.Object(in, 1))
	}
}

func normalizeObject(obj map[string]any) (any, error) {
	if obj == nil {
		return obj, nil
	}
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal object: %w", err)
	}

	return byteToType(data)
}

func byteToType(in []byte) (any, error) {
	if len(in) == 0 {
		return nil, errors.New("a valid Json document is expected")
	}

	switch in[0] {
	case '{':
		data := make(map[string]any)
		if err := json.Unmarshal(in, &data); err != nil {
			return nil, fmt.Errorf("unable to unmarshal result, %w", err)
		}

		return data, nil
	case '[':
		var data []any
		if err := json.Unmarshal(in, &data); err != nil {
			return nil, fmt.Errorf("unable to unmarshal result, %w", err)
		}

		return data, nil
	default:
		return nil, errors.New("a Json Array or Object is required")
	}
}
