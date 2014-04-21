package gorp

import (
	"log"
	"os"
	"testing"
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

	emptyInv := new(OverriddenInvoice)

	err = dbmap.Query(emptyInv).
		Set(&emptyInv.Id, "1").
		Set(&emptyInv.Created, 1).
		Set(&emptyInv.Updated, 1).
		Set(&emptyInv.Memo, "test_memo").
		Set(&emptyInv.PersonId, 1).
		Set(&emptyInv.IsPaid, false).
		Insert()
	if err != nil {
		t.Errorf("Failed to insert: %s", err)
		t.FailNow()
	}

	err = dbmap.Query(emptyInv).
		Set(&emptyInv.Id, "2").
		Set(&emptyInv.Created, 2).
		Set(&emptyInv.Updated, 2).
		Set(&emptyInv.Memo, "anoter_test_memo").
		Set(&emptyInv.PersonId, 2).
		Set(&emptyInv.IsPaid, false).
		Insert()
	if err != nil {
		t.Errorf("Failed to insert: %s", err)
		t.FailNow()
	}

	err = dbmap.Query(emptyInv).
		Set(&emptyInv.Id, "3").
		Set(&emptyInv.Created, 1).
		Set(&emptyInv.Updated, 3).
		Set(&emptyInv.Memo, "test_memo").
		Set(&emptyInv.PersonId, 1).
		Set(&emptyInv.IsPaid, false).
		Insert()
	if err != nil {
		t.Errorf("Failed to insert: %s", err)
		t.FailNow()
	}

	err = dbmap.Query(emptyInv).
		Set(&emptyInv.Id, "4").
		Set(&emptyInv.Created, 2).
		Set(&emptyInv.Updated, 1).
		Set(&emptyInv.Memo, "another_test_memo").
		Set(&emptyInv.PersonId, 1).
		Set(&emptyInv.IsPaid, false).
		Insert()
	if err != nil {
		t.Errorf("Failed to insert: %s", err)
		t.FailNow()
	}

	invTest, err := dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, true).
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 0 {
		t.Errorf("Expected zero paid invoices")
		t.FailNow()
	}

	count, err := dbmap.Query(emptyInv).
		Set(&emptyInv.IsPaid, true).
		Where().
		Equal(&emptyInv.Id, "4").
		Update()
	if err != nil {
		t.Errorf("Failed to update: %s", err)
		t.FailNow()
	}
	if count != 1 {
		t.Errorf("Expected to update one invoice")
		t.FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, true).
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 1 {
		t.Errorf("Expected one paid invoice")
		t.FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Greater(&emptyInv.Updated, 1).
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 2 {
		t.Errorf("Expected two inv")
		t.FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, true).
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 1 {
		t.Errorf("Expected one inv")
		t.FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, false).
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 3 {
		t.Errorf("Expected three inv")
		t.FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, false).
		Equal(&emptyInv.Created, 2).
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 1 {
		t.Errorf("Expected one inv")
		t.FailNow()
	}

	count, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, true).
		Delete()
	if err != nil {
		t.Errorf("Failed to delete: %s", err)
		t.FailNow()
	}
	if count != 1 {
		t.Errorf("Expected one invoice to be deleted")
		t.FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, true).
		Select()
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(invTest) != 0 {
		t.Errorf("Expected no paid invoices after deleting all paid invoices")
		t.FailNow()
	}
}

func BenchmarkSqlQuerySelect(b *testing.B) {
	b.StopTimer()
	dbmap := newDbMap()
	dbmap.Exec("drop table if exists OverriddenInvoice")
	dbmap.TraceOff()
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
	err = dbmap.Insert(inv)
	if err != nil {
		panic(err)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		q := "SELECT * FROM overriddeninvoice WHERE memo = $1"
		_, err = dbmap.Select(inv, q, "test_memo")
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkGorpQuerySelect(b *testing.B) {
	b.StopTimer()
	dbmap := newDbMap()
	dbmap.Exec("drop table if exists OverriddenInvoice")
	dbmap.TraceOff()
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
	err = dbmap.Insert(inv)
	if err != nil {
		panic(err)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		t := new(OverriddenInvoice)
		_, err := dbmap.Query(t).
			Where().
			Equal(&t.Memo, "test_memo").
			Select()
		if err != nil {
			panic(err)
		}
	}
}
