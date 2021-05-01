package flexmem

import (
	"fmt"
	"io"
	"reflect"

	"git.kanosolution.net/kano/dbflex"
	"github.com/ariefdarmawan/reflector"
)

type Cursor struct {
	dbflex.CursorBase

	records     []interface{}
	recordIndex int
}

func (cr *Cursor) Reset() error {
	return nil
}

func (cr *Cursor) Fetch(out interface{}) dbflex.ICursor {
	if cr.recordIndex >= len(cr.records) {
		return cr.SetError(io.EOF)
	}

	elem := cr.records[cr.recordIndex]
	if e := reflector.AssignValue(reflect.ValueOf(elem), reflect.ValueOf(out)); e != nil {
		return cr.SetError(fmt.Errorf("error on serializing fetch. %s", e.Error()))
	}

	cr.recordIndex++
	return cr
}

func (cr *Cursor) Fetchs(dest interface{}, n int) dbflex.ICursor {
	if cr.recordIndex >= len(cr.records) {
		return cr.SetError(io.EOF)
	}

	vdest := reflect.ValueOf(dest)
	if vdest.Kind() != reflect.Ptr {
		return cr.SetError(fmt.Errorf("destination should be pointer of slice"))
	}
	vdest = vdest.Elem()
	if vdest.Kind() != reflect.Slice {
		return cr.SetError(fmt.Errorf("destination should be pointer of slice"))
	}

	dataCount := len(cr.records)
	nRead := n
	if n == 0 {
		nRead = len(cr.records)
	}
	fetched := 0

	newDest := reflect.MakeSlice(vdest.Type(), nRead, nRead)
	newDest = reflect.New(newDest.Type())
	for (fetched < nRead) && (cr.recordIndex < dataCount) {
		elem := cr.records[cr.recordIndex]
		vElem := reflect.ValueOf(elem)
		if e := reflector.AssignSliceItem(vElem, fetched, newDest); e != nil {
			return cr.SetError(fmt.Errorf("error serializing during fetchs process. %s", e.Error()))
		}
		fetched++
		cr.recordIndex++
	}

	if fetched != nRead {
		newDest.Elem().SetCap(fetched)
		newDest.Elem().SetLen(fetched)
	}
	vdest.Set(newDest.Elem())

	return cr
}

func (cr *Cursor) Count() int {
	return len(cr.records)
}

func (cr *Cursor) Close() error {
	e := cr.Error()
	if e != nil {
		return e
	}
	return nil
}
