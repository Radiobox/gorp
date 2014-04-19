package gorp

import (
	"testing"
	"log"
	"os"
	"reflect"
)

func TestQueryLanguage(t *testing.T) {
	dbmap := newDbMap()
	dbmap.Exec("drop table if exists OverriddenInvoice")
	dbmap.TraceOn("", log.New(os.Stdout, "gorptest: ", log.Lmicroseconds))
	dbmap.AddTable(OverriddenInvoice{}).SetKeys(false, "Id")
	err := dbmap.CreateTablesIfNotExists()
	if err != nil {
		panic(err)
	}
	defer dropAndClose(dbmap)

	inv := &OverriddenInvoice{
		Id: "1",
		Invoice: Invoice{
			Created: 1,
			Updated: 1,
			Memo: "test_memo",
			PersonId: 1,
			IsPaid: false,
		},
	}
	inv2 := &OverriddenInvoice{
		Id: "2",
		Invoice: Invoice{
			Created: 2,
			Updated: 2,
			Memo: "another_test_memo",
			PersonId: 2,
			IsPaid: false,
		},
	}
	inv3 := &OverriddenInvoice{
		Id: "3",
		Invoice: Invoice{
			Created: 1,
			Updated: 3,
			Memo: "test_memo",
			PersonId: 1,
			IsPaid: false,
		},
	}
	inv4 := &OverriddenInvoice{
		Id: "4",
		Invoice: Invoice{
			Created: 2,
			Updated: 1,
			Memo: "another_test_memo",
			PersonId: 1,
			IsPaid: true,
		},
	}
	err = dbmap.Insert(inv, inv2, inv3, inv4)
	if err != nil {
		panic(err)
	}

	invTest, err := dbmap.Query(inv).
		Where().
		Equal(&inv.Memo, "test_memo").
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 2 {
		t.Errorf("Expected two inv")
		t.FailNow()
	}
	if !reflect.DeepEqual(inv, invTest[0]) {
		t.Errorf("%v != %v", inv, invTest[0])
	}
	if !reflect.DeepEqual(inv3, invTest[1]) {
		t.Errorf("%v != %v", inv3, invTest[1])
	}

	invTest, err = dbmap.Query(inv).
		Where().
		Greater(&inv.Updated, 1).
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 2 {
		t.Errorf("Expected two inv")
		t.FailNow()
	}

	invTest, err = dbmap.Query(inv).
		Where().
		Equal(&inv.IsPaid, true).
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 1 {
		t.Errorf("Expected one inv")
		t.FailNow()
	}

	invTest, err = dbmap.Query(inv).
		Where().
		Equal(&inv.IsPaid, false).
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 3 {
		t.Errorf("Expected three inv")
		t.FailNow()
	}

	invTest, err = dbmap.Query(inv).
		Where().
		Equal(&inv.IsPaid, false).
		Equal(&inv.Created, 2).
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 1 {
		t.Errorf("Expected one inv")
		t.FailNow()
	}
}
