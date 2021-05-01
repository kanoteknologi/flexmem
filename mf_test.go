package flexmem_test

import (
	"fmt"
	"testing"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/orm"
	"github.com/eaciit/toolkit"
	"github.com/kanoteknologi/flexmem"
	"github.com/smartystreets/goconvey/convey"
)

const (
	testCount = int(100)
	sample    = int(1000)
	randSeed  = 5000
)

func TestCRUD(t *testing.T) {
	convey.Convey("connection", t, func() {
		conn, e := dbflex.NewConnectionFromURI(flexmem.DriverName+"://localhost", nil)
		convey.So(e, convey.ShouldBeNil)
		e = conn.Connect()
		convey.So(e, convey.ShouldBeNil)
		defer conn.Close()

		e = flexmem.RegisterObject(new(Obj))
		convey.So(e, convey.ShouldBeNil)

		testID := int(3)
		obj := new(Obj)
		convey.Convey("insert object", func() {
			newid := fmt.Sprintf("user-manual-%d", testID)
			for i := 1; i <= testCount; i++ {
				insertID := fmt.Sprintf("user-manual-%d", i)
				insertObj := newObj(insertID, randSeed)
				insertObj.Index = i
				cmd := dbflex.From(insertObj.TableName()).Insert()
				_, e := conn.Execute(cmd, toolkit.M{}.Set("data", insertObj))
				convey.So(e, convey.ShouldBeNil)

				if testID == i {
					obj = insertObj
				}
			}

			convey.Convey("validate entry", func() {
				cmd := dbflex.From(obj.TableName()).Select()
				cr := conn.Cursor(cmd, nil)
				convey.So(cr.Count(), convey.ShouldEqual, testCount)
				cr.Close()

				mobjs := []Obj{}
				e := conn.Cursor(cmd, nil).Fetchs(&mobjs, 0).Close()
				convey.So(e, convey.ShouldBeNil)
				convey.So(cr.Count(), convey.ShouldEqual, testCount)
				//convey.Println("\nData:", toolkit.JsonStringIndent(mobjs[:3], "\t"))

				objs := []Obj{}
				cmd = dbflex.From(obj.TableName()).Where(dbflex.Gt("Index", 5)).Select()
				e = conn.Cursor(cmd, nil).Fetchs(&objs, 0).Close()
				convey.So(e, convey.ShouldBeNil)
				convey.So(len(objs), convey.ShouldEqual, testCount-5)

				cmd = dbflex.From(obj.TableName()).Where(dbflex.Range("Index", 20, 25)).Select()
				e = conn.Cursor(cmd, nil).Fetchs(&objs, 0).Close()
				convey.So(e, convey.ShouldBeNil)
				convey.So(len(objs), convey.ShouldEqual, 6)

				cmd = dbflex.From(obj.TableName()).Where(dbflex.Eq("ID", newid)).Select()
				insertedObj := new(Obj)
				e = conn.Cursor(cmd, nil).Fetch(insertedObj).Close()
				convey.So(e, convey.ShouldBeNil)
				convey.So(insertedObj.Seed, convey.ShouldEqual, obj.Seed)

				convey.Convey("update certain column", func() {
					insertedObj.Seed = 8000
					insertedObj.Date = time.Now().Add(30 * 24 * time.Hour)
					cmd := dbflex.From(obj.TableName()).Where(dbflex.Eq("ID", newid)).Update("Seed")
					_, e := conn.Execute(cmd, toolkit.M{}.Set("data", insertedObj))
					convey.So(e, convey.ShouldBeNil)

					convey.Convey("validate update", func() {
						cmd := dbflex.From(obj.TableName()).Where(dbflex.Eq("ID", newid)).Select()
						updatedObj := new(Obj)
						e := conn.Cursor(cmd, nil).Fetch(updatedObj).Close()
						convey.So(e, convey.ShouldBeNil)
						convey.So(updatedObj.Seed, convey.ShouldEqual, insertedObj.Seed)
						convey.So(updatedObj.Date, convey.ShouldEqual, obj.Date)

						cmd = dbflex.From(obj.TableName()).Where(dbflex.Ne("ID", newid)).Select()
						cursor := conn.Cursor(cmd, nil)
						defer cursor.Close()
						convey.So(cursor.Count(), convey.ShouldEqual, testCount-1)

						convey.Convey("delete", func() {
							cmd := dbflex.From(obj.TableName()).Where(dbflex.Eq("ID", newid)).Delete()
							_, e := conn.Execute(cmd, nil)
							convey.So(e, convey.ShouldBeNil)

							convey.Convey("validate delete", func() {
								cmd := dbflex.From(obj.TableName()).Select()
								cursor := conn.Cursor(cmd, nil)
								defer cursor.Close()
								convey.So(e, convey.ShouldBeNil)
								convey.So(cursor.Count(), convey.ShouldEqual, testCount-1)
							})
						})
					})
				})
			})
		})
	})
}

func TestGroup(t *testing.T) {
	convey.Convey("prepare", t, func() {
		conn, _ := dbflex.NewConnectionFromURI(flexmem.DriverName+"://localhost", nil)
		conn.Connect()
		flexmem.RegisterObject(new(Obj))

		convey.Convey("insert object", func() {
			for i := 1; i <= testCount; i++ {
				insertID := fmt.Sprintf("user-manual-%d", i)
				insertObj := newObj(insertID, randSeed)
				insertObj.Index = toolkit.RandInt(4) + 1
				cmd := dbflex.From(insertObj.TableName()).Insert()
				conn.Execute(cmd, toolkit.M{}.Set("data", insertObj))
			}

			results := []toolkit.M{}
			cmd := dbflex.From(new(Obj).TableName()).
				GroupBy("Index").
				Aggr(
					dbflex.NewAggrItem("SumSeed", dbflex.AggrSum, "Seed"),
					dbflex.NewAggrItem("AvgSeed", dbflex.AggrAvg, "Seed"),
					dbflex.NewAggrItem("CountSeed", dbflex.AggrCount, "Seed"),
					dbflex.NewAggrItem("MaxSeed", dbflex.AggrMax, "Seed"),
					dbflex.NewAggrItem("MinSeed", dbflex.AggrMin, "Seed"))
			cr := conn.Cursor(cmd, nil)
			e := cr.Fetchs(&results, 0).Close()
			convey.So(e, convey.ShouldBeNil)
			convey.So(len(results), convey.ShouldBeGreaterThan, 0)
			convey.Println("\nResult:", toolkit.JsonStringIndent(results, ""))
		})
	})
}

type Obj struct {
	orm.DataModelBase
	ID    string
	Index int
	Name  string
	Date  time.Time
	Seed  int
}

func (o *Obj) TableName() string {
	return "objs"
}

func (o *Obj) GetID(_ dbflex.IConnection) ([]string, []interface{}) {
	return []string{"ID"}, []interface{}{o.ID}
}

func newObj(id string, seed int) *Obj {
	obj := new(Obj)
	if id == "" {
		id = toolkit.RandomString(10)
	}
	obj.ID = id
	obj.Name = "Name " + obj.ID
	obj.Date = time.Now().Add(time.Millisecond + time.Duration(toolkit.RandInt(seed)))
	obj.Seed = toolkit.RandInt(seed)
	return obj
}
