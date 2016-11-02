package river

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/juju/errors"
	"../aerospike"
	"../go-mysql/canal"
	"../go-mysql/schema"
	"github.com/siddontang/go/log"
	"time"
	"strconv"
)

const (
	syncInsertDoc = iota
	syncDeleteDoc
	syncUpdateDoc
)

const (
	fieldTypeList = "list"
)


type rowsEventHandler struct {
	r *River
}

func (h *rowsEventHandler) Do(e *canal.RowsEvent) error {
	rule, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}
	var reqs []*AeroSpike.BulkRequest
	var err error
	switch e.Action {
	case canal.InsertAction:
		reqs, err = h.r.makeInsertRequest(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = h.r.makeDeleteRequest(rule, e.Rows)
	case canal.UpdateAction:
		reqs, err = h.r.makeUpdateRequest(rule, e.Rows)
	default:
		return errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		return errors.Errorf("make %s ES request err %v", e.Action, err)
	}

	if err := h.r.doBulk(reqs); err != nil {
		log.Errorf("do ES bulks err %v, stop", err)
		return canal.ErrHandleInterrupted
	}

	return nil
}

func (h *rowsEventHandler) String() string {
	return "ASRiverRowsEventHandler"
}

func getColumn(rule *Rule,row []interface{},name string) interface{}{
	idx:=rule.TableInfo.FindColumn(name)
	if idx == -1{
		log.Warnf("column %s not found in %s:%s",name,rule.Schema,rule.Table)
		return nil
	}
	return row[idx]
}

// for insert and delete
func (r *River) makeRequest(rule *Rule, action string, rows [][]interface{}) ([]*AeroSpike.BulkRequest, error) {
	reqs := make([]*AeroSpike.BulkRequest, 0, len(rows))
	for _, values := range rows {
		id, err := r.getDocID(rule, values)
		if err != nil {
			return nil, errors.Trace(err)
		}
		req := &AeroSpike.BulkRequest{Namespace: rule.Index, Set: rule.Type, Id: id}

		if action == canal.DeleteAction {
			req.Action = AeroSpike.DELETE
			r.st.DeleteNum.Add(1)
		} else {
			r.makeInsertReqData(req, rule, values)
			r.st.InsertNum.Add(1)
		}
		reqs = append(reqs, req)

		equivIds := rule.EquivIds
		if len(equivIds)!=0 && action!=canal.DeleteAction { //has equivalent Ids,forge id mapping request
			idValues:=make([]string,0,20)
			for _,equivId:=range equivIds{
				idValues = append(idValues,toString(getColumn(rule,values,equivId)))
			}
			mappingId:=makeDocumentId(idValues)
			mappingReq := &AeroSpike.BulkRequest{Namespace:rule.Index,Set:rule.MappingType,Id:mappingId}
			mappingReq.Data = make(map[string]interface{},20)
			mappingReq.Data["value"] = fmt.Sprintf("%v",id)
			mappingReq.Action = AeroSpike.UPDATE
			reqs = append(reqs,mappingReq)
		}
	}

	return reqs, nil
}

func (r *River) makeInsertRequest(rule *Rule, rows [][]interface{}) ([]*AeroSpike.BulkRequest, error) {
	return r.makeRequest(rule, canal.InsertAction, rows)
}

func (r *River) makeDeleteRequest(rule *Rule, rows [][]interface{}) ([]*AeroSpike.BulkRequest, error) {
	return r.makeRequest(rule, canal.DeleteAction, rows)
}

func (r *River) makeUpdateRequest(rule *Rule, rows [][]interface{}) ([]*AeroSpike.BulkRequest, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	reqs := make([]*AeroSpike.BulkRequest, 0, len(rows))

	for i := 0; i < len(rows); i += 2 {
		beforeID, err := r.getDocID(rule, rows[i])
		if err != nil {
			return nil, errors.Trace(err)
		}

		afterID, err := r.getDocID(rule, rows[i+1])

		if err != nil {
			return nil, errors.Trace(err)
		}

		req := &AeroSpike.BulkRequest{Namespace: rule.Index, Set: rule.Type, Id: beforeID}

		if beforeID != afterID{
			req.Action = AeroSpike.DELETE
			reqs = append(reqs, req)

			req = &AeroSpike.BulkRequest{Namespace: rule.Index, Set: rule.Type, Id: afterID}
			r.makeInsertReqData(req, rule, rows[i+1])

			r.st.DeleteNum.Add(1)
			r.st.InsertNum.Add(1)
			reqs = append(reqs, req)
		} else {
			if r.makeUpdateReqData(req, rule, rows[i], rows[i+1]) {
				r.st.UpdateNum.Add(1)
				reqs = append(reqs, req)
			}
		}
	}

	return reqs, nil
}

func (r *River) makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	}

	return value
}

func (r *River) getFieldParts(k string, v string) (string, string, string) {
	composedField := strings.Split(v, ",")

	mysql := k
	as := composedField[0]
	fieldType := ""

	if 0 == len(as) {
		as = mysql
	}
	if 2 == len(composedField) {
		fieldType = composedField[1]
	}

	return mysql, as, fieldType
}

func (r *River) makeInsertReqData(req *AeroSpike.BulkRequest, rule *Rule, values []interface{}) {
	req.Data = make(map[string]interface{}, len(values))
	req.Action = AeroSpike.INSERT
	fieldSet := rule.FilteredFieldSet
	for i, c := range rule.TableInfo.Columns {
		if _,ok := fieldSet[c.Name];len(fieldSet)!=0&&!ok{ //not in filetered fields and filtered set not empty
			continue
		}
		mapped := false
		for k, v := range rule.FieldMapping {
			mysql, as, fieldType := r.getFieldParts(k, v)
			if mysql == c.Name {
				mapped = true
				v := r.makeReqColumnData(&c, values[i])
				if fieldType == fieldTypeList {
					if str, ok := v.(string); ok {
						req.Data[as] = strings.Split(str, ",")
					} else {
						req.Data[as] = v
					}
				} else {
					req.Data[as] = v
				}
			}
		}
		if mapped == false {
			req.Data[c.Name] = r.makeReqColumnData(&c, values[i])
		}
	}
}

func (r *River) makeUpdateReqData(req *AeroSpike.BulkRequest, rule *Rule,
	beforeValues []interface{}, afterValues []interface{}) bool {
	req.Data = make(map[string]interface{}, len(beforeValues))

	// maybe dangerous if something wrong delete before?
	req.Action = AeroSpike.UPDATE
	fieldSet := rule.FilteredFieldSet
	for i, c := range rule.TableInfo.Columns {
		if _,ok := fieldSet[c.Name];len(fieldSet)!=0&&!ok{ //not in filetered fields and filtered set not empty
			continue
		}
		mapped := false
		if reflect.DeepEqual(beforeValues[i], afterValues[i]) {
			//nothing changed
			continue
		}
		for k, v := range rule.FieldMapping {
			mysql, as, fieldType := r.getFieldParts(k, v)
			if mysql == c.Name {
				mapped = true
				// has custom field mapping
				v := r.makeReqColumnData(&c, afterValues[i])
				str, ok := v.(string)
				if ok == false {
					req.Data[c.Name] = v
				} else {
					if fieldType == fieldTypeList {
						req.Data[as] = strings.Split(str, ",")
					} else {
						req.Data[as] = str
					}
				}
			}
		}
		if mapped == false {
			req.Data[c.Name] = r.makeReqColumnData(&c, afterValues[i])
		}

	}
	if len(req.Data) == 0 { //nothing concerned change
		return false
	}
	return true
}

func makeDocumentId(input []string) string{
	return strings.Join(input,":")
}

func toString(input interface{}) string{
	switch input.(type){
	case int64:
		return strconv.FormatInt(input.(int64),10)
	case int32:
		return strconv.FormatInt(int64(input.(int32)),10)
	case int16:
		return strconv.FormatInt(int64(input.(int16)),10)
	case int8:
		return strconv.FormatInt(int64(input.(int8)),10)
	case int:
		return strconv.FormatInt(int64(input.(int)),10)
	case uint64:
		return strconv.FormatUint(input.(uint64),10)
	case uint32:
		return strconv.FormatUint(uint64(input.(uint32)),10)
	case uint16:
		return strconv.FormatUint(uint64(input.(uint16)),10)
	case uint8:
		return strconv.FormatUint(uint64(input.(uint8)),10)
	case uint:
		return strconv.FormatUint(uint64(input.(uint)),10)
	case string:
		return input.(string)
	case float64:
		return strconv.FormatFloat(input.(float64),'e',5,64)
	case float32:
		return strconv.FormatFloat(float64(input.(float32)),'e',5,64)
	case bool:
		return strconv.FormatBool(input.(bool))
	default:
		return ""
	}
}

// Get primary keys in one row and format them into a string
// PK must not be nil
func (r *River) getDocID(rule *Rule, row []interface{}) (interface{}, error) {
	var docId interface{}
	if len(rule.Ids) != 0 {
		idValues:=make([]string,0,10)
		for _,id:=range rule.Ids {
			idElement:=rule.TableInfo.FindColumn(id)
			idValues[len(idValues)] = toString(row[idElement])
		}
		docId = makeDocumentId(idValues)
	}else {
		pks, err := canal.GetPKValues(rule.TableInfo, row)
		if err != nil {
			return "", err
		}

		if len(pks) > 1 {
			var buf bytes.Buffer
			sep := ""
			for i, value := range pks {
				if value == nil {
					return "", errors.Errorf("The %ds PK value is nil", i)
				}

				buf.WriteString(fmt.Sprintf("%s%v", sep, value))
				sep = ":"
			}
			docId = buf.String()
		}else{
			docId = pks[0]
		}
	}
	return docId, nil
}

func (r *River) getParentID(rule *Rule, row []interface{}, columnName string) (string, error) {
	index := rule.TableInfo.FindColumn(columnName)
	if index < 0 {
		return "", errors.Errorf("parent id not found %s(%s)", rule.TableInfo.Name, columnName)
	}
	var pId string
	pId = fmt.Sprint(row[index])
	return pId, nil
}

func makeMQMessage(req *AeroSpike.BulkRequest) string{
	return fmt.Sprintf("u|%s|%s|%v",req.Namespace,req.Set,req.Id)
}

func (r *River) doBulk(reqs []*AeroSpike.BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}

	if _, err := r.as.DoBulk(reqs); err != nil {
		log.Errorf("sync docs err %v after binlog %s", err, r.canal.SyncedPosition())
		return errors.Trace(err)
	}
	//MQ
	timeNow := time.Now()
	timestamp:=timeNow.Unix()*1000
	timeString:=timeNow.String()
	mqService:=r.mqService
	for _,item:=range reqs {
		msgBody:=makeMQMessage(item)
		fmt.Printf("time:%s,msgbody:%s\n",timeString,msgBody)
		mqService.SendMessage(msgBody,timestamp)
	}
	return nil
}
