package flexmem

import (
	"context"

	"git.kanosolution.net/kano/dbflex"
)

type Connection struct {
	dbflex.ConnectionBase `bson:"-" json:"-"`
	ctx                   context.Context
	state                 string

	records []interface{}
	index   int
}

func (conn *Connection) Connect() error {
	conn.state = dbflex.StateConnected
	return nil
}

func (conn *Connection) State() string {
	return conn.state
}

func (conn *Connection) Close() {
	conn.state = ""
}

func (conn *Connection) NewQuery() dbflex.IQuery {
	qr := new(Query)
	qr.SetThis(qr)
	qr.SetConnection(conn)
	return qr
}

func (conn *Connection) ObjectNames(_ dbflex.ObjTypeEnum) []string {
	res := make([]string, len(tables))
	i := 0
	for k := range tables {
		res[i] = k
		i++
	}
	return res
}

func (conn *Connection) ValidateTable(_ interface{}, _ bool) error {
	return nil
}

func (conn *Connection) DropTable(name string) error {
	lock.Lock()
	delete(tables, name)
	lock.Unlock()
	return nil
}

func (conn *Connection) HasTable(name string) bool {
	_, ok := tables[name]
	return ok
}

func (conn *Connection) EnsureTable(_ string, _ []string, _ interface{}) error {
	return nil
}

func (conn *Connection) BeginTx() error {
	panic("not implemented") // TODO: Implement
}

func (conn *Connection) Commit() error {
	panic("not implemented") // TODO: Implement
}

func (conn *Connection) RollBack() error {
	panic("not implemented") // TODO: Implement
}

func (conn *Connection) SupportTx() bool {
	panic("not implemented") // TODO: Implement
}

func (conn *Connection) IsTx() bool {
	panic("not implemented") // TODO: Implement
}
