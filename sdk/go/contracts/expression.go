package contracts

import "encoding/json"

// ValueExpr is a closed structured AST. Exactly one variant must be present.
// Literal is a pointer so the JSON literal null remains distinct from absence.
type ValueExpr struct {
	Literal *json.RawMessage     `json:"literal,omitempty"`
	Ref     string               `json:"ref,omitempty"`
	Op      string               `json:"op,omitempty"`
	Args    []ValueExpr          `json:"args,omitempty"`
	Object  map[string]ValueExpr `json:"object,omitempty"`
	Array   *[]ValueExpr         `json:"array,omitempty"`
}

type ValueBinding struct {
	Target string    `json:"target"`
	Value  ValueExpr `json:"value"`
}
