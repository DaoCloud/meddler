package meddler

import (
	"strings"
	"testing"
)

type ItemJson struct {
	ID     int64           `meddler:"id,pk"`
	Stuff  map[string]bool `meddler:"stuff,json"`
	StuffZ map[string]bool `meddler:"stuffz,jsongzip"`
}

type ItemGob struct {
	ID     int64           `meddler:"id,pk"`
	Stuff  map[string]bool `meddler:"stuff,gob"`
	StuffZ map[string]bool `meddler:"stuffz,gobgzip"`
}

type UuidJson struct {
	ID   string `meddler:"id,pk"`
	Name string `meddler:"name"`
	Age  int    `meddler:"age"`
}

func init() {
	SetTagMapper(strings.ToLower)
}

func TestJsonMeddler(t *testing.T) {
	once.Do(setup)

	// save a value
	elt := &ItemJson{
		ID: 0,
		Stuff: map[string]bool{
			"hello": true,
			"world": true,
		},
		StuffZ: map[string]bool{
			"goodbye": true,
			"cruel":   true,
			"world":   true,
		},
	}

	if err := Save(db, "item", elt); err != nil {
		t.Errorf("Save error: %v", err)
	}
	id := elt.ID

	// load it again
	elt = new(ItemJson)
	if err := Load(db, "item", elt, id); err != nil {
		t.Errorf("Load error: %v", err)
	}

	if elt.ID != id {
		t.Errorf("expected id of %d, found %d", id, elt.ID)
	}
	if len(elt.Stuff) != 2 {
		t.Errorf("expected %d items in Stuff, found %d", 2, len(elt.Stuff))
	}
	if !elt.Stuff["hello"] || !elt.Stuff["world"] {
		t.Errorf("contents of stuff wrong: %v", elt.Stuff)
	}
	if len(elt.StuffZ) != 3 {
		t.Errorf("expected %d items in StuffZ, found %d", 3, len(elt.StuffZ))
	}
	if !elt.StuffZ["goodbye"] || !elt.StuffZ["cruel"] || !elt.StuffZ["world"] {
		t.Errorf("contents of stuffz wrong: %v", elt.StuffZ)
	}
	if _, err := db.Exec("delete from `item`"); err != nil {
		t.Errorf("error wiping item table: %v", err)
	}
}

func TestGobMeddler(t *testing.T) {
	once.Do(setup)

	// save a value
	elt := &ItemGob{
		ID: 0,
		Stuff: map[string]bool{
			"hello": true,
			"world": true,
		},
		StuffZ: map[string]bool{
			"goodbye": true,
			"cruel":   true,
			"world":   true,
		},
	}

	if err := Save(db, "item", elt); err != nil {
		t.Errorf("Save error: %v", err)
	}
	id := elt.ID

	// load it again
	elt = new(ItemGob)
	if err := Load(db, "item", elt, id); err != nil {
		t.Errorf("Load error: %v", err)
	}

	if elt.ID != id {
		t.Errorf("expected id of %d, found %d", id, elt.ID)
	}
	if len(elt.Stuff) != 2 {
		t.Errorf("expected %d items in Stuff, found %d", 2, len(elt.Stuff))
	}
	if !elt.Stuff["hello"] || !elt.Stuff["world"] {
		t.Errorf("contents of stuff wrong: %v", elt.Stuff)
	}
	if len(elt.StuffZ) != 3 {
		t.Errorf("expected %d items in StuffZ, found %d", 3, len(elt.StuffZ))
	}
	if !elt.StuffZ["goodbye"] || !elt.StuffZ["cruel"] || !elt.StuffZ["world"] {
		t.Errorf("contents of stuffz wrong: %v", elt.StuffZ)
	}
	if _, err := db.Exec("delete from `item`"); err != nil {
		t.Errorf("error wiping item table: %v", err)
	}
}

func Test_UUID(t *testing.T) {
	once.Do(setup)

	man := UuidJson{
		Name: "Tom",
		Age:  18,
	}

	if err := Save(db, "men", &man); err != nil {
		t.Errorf("Save error: %v", err)
	}

	if man.ID == "" {
		t.Error("Save error: pk should not be empty")
	}

	others := UuidJson{}
	if err := Load(db, "men", &others, man.ID); err != nil {
		t.Error("Load error: ", err)
	}

	if others.Age != man.Age || others.Name != man.Name {
		t.Errorf("Load error: %#v <=> %#v", man, others)
	}

	man.Age++
	another := UuidJson{}
	if err := Save(db, "men", &man); err != nil {
		t.Errorf("Save error: %v", err)
	}

	if err := Load(db, "men", &another, man.ID); err != nil {
		t.Error("Load error: ", err)
	}

	if others.Age+1 != another.Age || others.Name != another.Name {
		t.Errorf("Load error: %#v <=> %#v", man, others)
	}

	if _, err := db.Exec("delete from `men`"); err != nil {
		t.Errorf("error wiping item table: %v", err)
	}
}
