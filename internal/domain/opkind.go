package domain

// OpKind is the kind of work an operation asks the node to do.
type OpKind string

// Allowed values for OpKind.
const (
	OpCreate OpKind = "create"
	OpDelete OpKind = "delete"
)
