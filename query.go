package flexmem

import (
	"errors"
	"fmt"
	"reflect"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/orm"
	"git.kanosolution.net/koloni/crowd"
	"github.com/ariefdarmawan/reflector"
	"github.com/eaciit/toolkit"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Query struct {
	dbflex.QueryBase
	conn *Connection
}

type MemFilterFunc func(ref *reflector.Reflector) bool

func (qr *Query) BuildFilter(f *dbflex.Filter) (interface{}, error) {
	var (
		fn     MemFilterFunc
		scanFn ScanFunc
		e      error
	)
	fn, e = qr.buildFilterFunc(f)
	if e != nil {
		return nil, e
	}
	if fn != nil {
		scanFn = ScanFunc(func(k string, r interface{}) (bool, interface{}) {
			ref := reflector.From(r)
			ok := fn(ref)
			if ok {
				return true, r
			}
			return false, nil
		})
		return scanFn, nil
	}
	return nil, nil
}

func (qr *Query) buildFilterFunc(f *dbflex.Filter) (MemFilterFunc, error) {
	fieldName := f.Field

	switch f.Op {
	case dbflex.OpAnd:
		items := f.Items
		fns := make([]MemFilterFunc, len(items))
		for itemIdx, item := range items {
			if buildItem, e := qr.BuildFilter(item); e == nil {
				return nil, errors.New("error when creating filter. " + e.Error())
			} else {
				fns[itemIdx] = buildItem.(MemFilterFunc)
			}
		}

		fn := func(ref *reflector.Reflector) bool {
			for _, mf := range fns {
				if !mf(ref) {
					return false
				}
			}
			return true
		}

		return MemFilterFunc(fn), nil

	case dbflex.OpOr:
		items := f.Items
		fns := make([]MemFilterFunc, len(items))
		for itemIdx, item := range items {
			if buildItem, e := qr.BuildFilter(item); e == nil {
				return nil, errors.New("error when creating filter. " + e.Error())
			} else {
				fns[itemIdx] = buildItem.(MemFilterFunc)
			}
		}

		fn := func(ref *reflector.Reflector) bool {
			for _, mf := range fns {
				if mf(ref) {
					return true
				}
			}
			return false
		}

		return MemFilterFunc(fn), nil

	case dbflex.OpEq:
		fn := func(ref *reflector.Reflector) bool {
			v, e := ref.Get(fieldName)
			if e != nil {
				return false
			}
			return v == f.Value
		}
		return MemFilterFunc(fn), nil

	case dbflex.OpNe:
		fn := func(ref *reflector.Reflector) bool {
			v, e := ref.Get(fieldName)
			if e != nil {
				return false
			}
			return v != f.Value
		}
		return MemFilterFunc(fn), nil

	case dbflex.OpGt:
		return compare(fieldName, string(f.Op), f.Value), nil

	case dbflex.OpGte:
		return compare(fieldName, string(f.Op), f.Value), nil

	case dbflex.OpLt:
		return compare(fieldName, string(f.Op), f.Value), nil

	case dbflex.OpLte:
		return compare(fieldName, string(f.Op), f.Value), nil

	case dbflex.OpRange:
		return MemFilterFunc(func(ref *reflector.Reflector) bool {
			v, e := ref.Get(fieldName)
			if e != nil {
				return false
			}

			rv := reflect.ValueOf(f.Value)
			if rv.Kind() != reflect.Slice {
				return false
			}

			if gte := toolkit.Compare(v, rv.Index(0).Interface(), "$gte"); gte {
				return toolkit.Compare(v, rv.Index(1).Interface(), "lte")
			}
			return false
		}), nil
	}

	return nil, nil
}

func compare(fieldName, op string, value interface{}) MemFilterFunc {
	return MemFilterFunc(func(ref *reflector.Reflector) bool {
		ret := false
		v, e := ref.Get(fieldName)
		if e != nil {
			return ret
		}
		return toolkit.Compare(v, value, op)
	})
}

func (qr *Query) BuildCommand() (interface{}, error) {
	return nil, nil
}

func (qr *Query) Cursor(parm toolkit.M) dbflex.ICursor {
	cr := new(Cursor)

	var (
		hasTable bool
		table    *memTable
	)

	tableName := qr.Config(dbflex.ConfigKeyTableName, "").(string)
	if tableName == "" {
		return cr.SetError(errors.New("tablename is missing"))
	}

	lock.RLock()
	table, hasTable = tables[tableName]
	lock.RUnlock()

	if !hasTable {
		return cr.SetError(fmt.Errorf("table %s is not registered yet", tableName))
	}

	where, hasWhere := qr.Config(dbflex.ConfigKeyWhere, nil).(ScanFunc)
	if !hasWhere {
		cr.records = table.RecordsAsArray()
	} else {
		cScan := table.Scan(where)
		for record := range cScan {
			cr.records = append(cr.records, record)
		}
	}

	qis := qr.Config(dbflex.ConfigKeyGroupedQueryItems, dbflex.QueryItems{}).(dbflex.QueryItems)
	groupObj, hasGroup := qis[dbflex.QueryGroup]
	aggrObj, hasAggr := qis[dbflex.QueryAggr]

	if !hasGroup && !hasAggr {
		return cr
	}

	datas := cr.records
	mre := crowd.FromSlice(datas)
	if hasGroup {
		groupNames, ok := groupObj.Value.([]string)
		if ok && len(groupNames) > 0 {
			mre.Group(func(record interface{}) interface{} {
				ref := reflector.From(record)
				res, _ := ref.Get(groupNames[0])
				return res
			})
		} else {
			hasGroup = false
		}
	}

	if !hasGroup {
		mre.Group(func(record interface{}) interface{} {
			return ""
		})
	}

	if hasAggr {
		aggrItems := aggrObj.Value.([]*dbflex.AggrItem)
		mre.Map(func(k interface{}, vals []interface{}) toolkit.M {
			m := toolkit.M{}.Set("Key", k)
			for _, v := range vals {
				dataRef := reflector.From(v)

				for _, aggrItem := range aggrItems {
					fv, e := dataRef.Get(aggrItem.Field)
					if e != nil {
						continue
					}

					switch aggrItem.Op {
					case dbflex.AggrSum:
						vfloat := toolkit.ToFloat64(fv, 10, toolkit.RoundingAuto)
						sum := m.GetFloat64(aggrItem.Alias)
						sum = sum + vfloat
						m.Set(aggrItem.Alias, sum)

					case dbflex.AggrCount:
						count := m.GetInt(aggrItem.Alias)
						count++
						m.Set(aggrItem.Alias, count)

					case dbflex.AggrAvg:
						vfloat := toolkit.ToFloat64(fv, 10, toolkit.RoundingAuto)
						count := m.GetFloat64(aggrItem.Alias + "_count")
						sum := m.GetFloat64(aggrItem.Alias + "_sum")
						count++
						sum = sum + vfloat
						m.Set(aggrItem.Alias+"_count", count)
						m.Set(aggrItem.Alias+"_sum", sum)
						m.Set(aggrItem.Alias, sum/count)

					case dbflex.AggrMin:
						vfloat := toolkit.ToFloat64(fv, 10, toolkit.RoundingAuto)
						var val float64
						if m.Has(aggrItem.Alias) {
							val = m.GetFloat64(aggrItem.Alias)
						} else {
							val = vfloat
							m.Set(aggrItem.Alias, vfloat)
						}
						if toolkit.Compare(vfloat, val, "$lt") {
							m.Set(aggrItem.Alias, vfloat)
						}

					case dbflex.AggrMax:
						vfloat := toolkit.ToFloat64(fv, 10, toolkit.RoundingAuto)
						val := m.GetFloat64(aggrItem.Alias)
						if toolkit.Compare(vfloat, val, "$gt") {
							m.Set(aggrItem.Alias, vfloat)
						}
					}
				}
			}

			for _, aggrItem := range aggrItems {
				if aggrItem.Op == dbflex.AggrAvg {
					m.Unset(aggrItem.Alias + "_count")
					m.Unset(aggrItem.Alias + "_sum")
				}
			}

			return m
		})
	}

	if recs, e := mre.Collect().Exec(); e != nil {
		return cr.SetError(e)
	} else {
		recm := recs.([]toolkit.M)
		cr.records = make([]interface{}, len(recm))
		for idx, m := range recm {
			cr.records[idx] = m
		}
		return cr
	}
}

func (qr *Query) Execute(m toolkit.M) (interface{}, error) {
	var (
		table        *memTable
		hasTable, ok bool
		odata        orm.DataModel
		e            error
	)

	parts := qr.Config(dbflex.ConfigKeyGroupedQueryItems, dbflex.QueryItems{}).(dbflex.QueryItems)
	data, hasData := m["data"]
	if hasData {
		odata, ok = data.(orm.DataModel)
		if !ok {
			return nil, errors.New("data need to implements orm.datamodel")
		}
	}

	tableName := qr.Config(dbflex.ConfigKeyTableName, "").(string)
	if tableName == "" {
		return nil, errors.New("tablename is required")
	}

	lock.RLock()
	table, hasTable = tables[tableName]
	lock.RUnlock()

	if !hasTable {
		return nil, fmt.Errorf("table %s is not registered yet", tableName)
	}

	where, _ := qr.Config(dbflex.ConfigKeyWhere, nil).(ScanFunc)

	ct := qr.Config(dbflex.ConfigKeyCommandType, "N/A")
	switch ct {
	case dbflex.QueryInsert:
		if !hasData {
			return nil, errors.New("data is missing")
		}

		_, rids := odata.GetID(qr.Connection())
		if rids[0].(string) == "" {
			rids[0] = primitive.NewObjectID().Hex()
			odata.SetID(rids...)
		}
		if e = table.Set(rids[0].(string), data, false); e != nil {
			return nil, e
		}
		return odata, nil

	case dbflex.QueryUpdate:
		if !hasData {
			return nil, errors.New("data is missing")
		}

		//-- get the field for update
		pq, _ := parts[dbflex.QueryUpdate]
		fieldNames := pq.Value.([]string)

		cScan := table.Scan(where)
		sourceRef := reflector.From(data)
		for rec := range cScan {
			targetRef := reflector.From(rec)
			orec, recOK := rec.(orm.DataModel)
			if !recOK {
				return nil, errors.New("invalid data to be updated")
			}

			_, rids := orec.GetID(qr.Connection())
			//-- update all object with new one
			if len(fieldNames) == 0 {
				table.Set(rids[0].(string), odata, true)
			} else { //or only certain field(s)
				for _, fieldName := range fieldNames {
					if getv, e := sourceRef.Get(fieldName); e == nil {
						targetRef.Set(fieldName, getv)
					}
				}
				targetRef.Flush()
				table.Set(rids[0].(string), rec, true)
			}
		}

		return odata, nil

	case dbflex.QueryDelete:
		deletedCount := 0
		cScan := table.Scan(where)
		for rec := range cScan {
			orec, recOK := rec.(orm.DataModel)
			if !recOK {
				continue
			}
			_, rids := orec.GetID(qr.Connection())
			table.Delete(rids[0].(string))
			deletedCount++
		}
		return deletedCount, nil

	default:
		return nil, fmt.Errorf("command %v is not valid", ct)
	}
}
