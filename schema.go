package maat

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/invopop/jsonschema"
	"google.golang.org/protobuf/types/known/structpb"
)

// SchemaFor 根据 Go 类型 T 生成工具参数的 JSON Schema（基于 github.com/invopop/jsonschema），
// 传给 NewTool 的 schema 参数。T 通常是参数结构体：
//
//	type ReadFileArgs struct {
//		Path string `json:"path" jsonschema:"description=工作区内的相对路径"`
//	}
//	maat.NewTool("read_file", "读取工作区内的文件", maat.SchemaFor[ReadFileArgs](), readFile)
//
// 顶层结构体直接展开为 type: object；没有 omitempty 的字段是必填字段；不允许额外字段。
// 嵌套的结构体放在 $defs 中引用。返回值可以在传给 NewTool 之前修改（例如补充描述）。
func SchemaFor[T any]() *jsonschema.Schema {
	t := reflect.TypeFor[T]()
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	// ExpandedStruct 只适用于具名结构体；其他类型（map、匿名结构体）本来就内联生成。
	r := jsonschema.Reflector{ExpandedStruct: t.Kind() == reflect.Struct && t.Name() != "", Anonymous: true}
	s := r.ReflectFromType(t)
	s.Version = "" // 不输出 $schema：部分模型供应商不接受
	return s
}

// schemaStruct 把 NewTool 的 schema 参数转换为 ToolDefinition.parameters。nil 表示工具没有参数。
func schemaStruct(schema any) (*structpb.Struct, error) {
	var raw []byte
	switch s := schema.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if s == nil {
			return nil, nil
		}
		raw = s
	case map[string]any:
		if s == nil {
			return nil, nil
		}
		b, err := json.Marshal(s)
		if err != nil {
			return nil, fmt.Errorf("encode schema: %w", err)
		}
		raw = b
	case *jsonschema.Schema:
		if s == nil {
			return nil, nil
		}
		b, err := json.Marshal(s)
		if err != nil {
			return nil, fmt.Errorf("encode schema: %w", err)
		}
		raw = b
	default:
		return nil, fmt.Errorf("unsupported schema type %T (use json.RawMessage, map[string]any or maat.SchemaFor)", schema)
	}
	// 统一经过 JSON 解码，数字等取值与平台看到的一致。
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("schema is not a JSON object: %w", err)
	}
	if dec.More() {
		return nil, errors.New("schema has trailing data after the JSON object")
	}
	if m["type"] != "object" {
		return nil, errors.New(`schema must have "type": "object" at the top level`)
	}
	st, err := structpb.NewStruct(m)
	if err != nil {
		return nil, fmt.Errorf("convert schema: %w", err)
	}
	return st, nil
}
