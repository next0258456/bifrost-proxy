package domain

// TableIdentifier encapsulates the namespace and table name into a single domain value
type TableIdentifier struct {
	Namespace string `json:"namespace"`
	Table     string `json:"table"`
}

func NewTableIdentifier(namespace, table string) TableIdentifier {
	return TableIdentifier{
		Namespace: namespace,
		Table:     table,
	}
}

func (t TableIdentifier) String() string {
	return t.Namespace + "." + t.Table
}

func (t TableIdentifier) S3Path() string {
	return t.Namespace + "/" + t.Table
}
