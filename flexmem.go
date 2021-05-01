package flexmem

import (
	"fmt"
	"sync"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/orm"
)

var (
	lock   *sync.RWMutex
	tables map[string]*memTable
)

const (
	DriverName = "flexmem"
)

func init() {
	//=== sample: text://localhost?path=/usr/local/txt
	lock = new(sync.RWMutex)
	tables = make(map[string]*memTable)

	dbflex.RegisterDriver(DriverName, func(si *dbflex.ServerInfo) dbflex.IConnection {
		c := new(Connection)
		return c.SetThis(c)
	})

	//fmt.Println("driver", DriverName, "has been registered successfully")
}

func RegisterObject(data interface{}) error {
	odata, ok := data.(orm.DataModel)
	if !ok {
		return fmt.Errorf("object need to implements orm.datamodel")
	}

	mt := newMemTable()
	mt.name = odata.TableName()
	tables[mt.name] = mt
	return nil
}
