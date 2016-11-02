package river

import (
	"../go-mysql/schema"
)

type Set map[string]bool

// If you want to sync MySQL data into elasticsearch, you must set a rule to let use know how to do it.
// The mapping rule may thi: schema + table <-> index + document type.
// schema and table is for MySQL, index and document type is for Elasticsearch.
type Rule struct {
	Schema string `toml:"schema"`
	Table  string `toml:"table"`
	Index  string `toml:"index"`
	Type   string `toml:"type"`
	Parent string `toml:"parent"`
	Ids    []string `toml:"ids"`
	EquivIds []string `toml:"equivalent_ids"`
	MappingType string `toml:"mappingtype"`
	FilteredFields []string `toml:"fields"`
	FilteredFieldSet Set
	// Default, a MySQL table field name is mapped to Elasticsearch field name.
	// Sometimes, you want to use different name, e.g, the MySQL file name is title,
	// but in Elasticsearch, you want to name it my_title.
	FieldMapping map[string]string `toml:"field"`

	// MySQL table information
	TableInfo *schema.Table
}

func newDefaultRule(schema string, table string) *Rule {
	r := new(Rule)

	r.Schema = schema
	r.Table = table
	r.Index = table
	r.Type = table
	r.FieldMapping = make(map[string]string)
	r.Ids = make([]string,0,10)
	r.EquivIds = make([]string,0,20)
	r.MappingType = table
	r.FilteredFields = make([]string,0,20)
	r.FilteredFieldSet = make(Set,20)
	return r
}

func (r *Rule) prepare() error {
	if r.FieldMapping == nil {
		r.FieldMapping = make(map[string]string)
	}

	if r.Ids == nil {
		r.Ids = make([]string,0,10)
	}

	if r.EquivIds == nil {
		r.EquivIds = make([]string,0,20)
	}

	if r.FilteredFields == nil{
		r.FilteredFields = make([]string,0,20)
	}

	if r.FilteredFieldSet == nil{
		r.FilteredFieldSet = make(Set,20)
	}
	for _,id:=range r.FilteredFields {
		r.FilteredFieldSet[id] = true
	}

	if len(r.Index) == 0 {
		r.Index = r.Table
	}

	if len(r.Type) == 0 {
		r.Type = r.Index
	}

	if len(r.MappingType) == 0{
		r.MappingType = r.Type
	}

	return nil
}
