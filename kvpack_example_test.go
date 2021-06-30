package kvpack_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/diamondburned/kvpack"
	"github.com/diamondburned/kvpack/driver/bboltpack"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

func mustDB(namespaces ...string) *kvpack.Database {
	path := filepath.Join(os.TempDir(), namespaces[0]+".db")
	opts := bbolt.DefaultOptions

	d, err := bboltpack.Open(path, os.ModePerm, opts)
	if err != nil {
		log.Fatalln("failed to open db:", err)
	}

	return d.WithNamespace(namespaces...)
}

func ExampleTransaction_Each() {
	type User struct {
		ID    int
		Name  string
		Color string
	}

	db := mustDB("kvpack-demo", "each-example")
	userDB := db.Descend("users")

	if err := userDB.Update(func(tx *kvpack.Transaction) error {
		users := []User{
			{0, "diamondburned", "pink"},
			{1, "somebody else", "cyan"},
			{2, "want this user", "red"},
		}

		for _, user := range users {
			if err := tx.Put([]byte(strconv.Itoa(user.ID)), &user); err != nil {
				return errors.Wrapf(err, "failed to put user %d", user.ID)
			}
		}
		return nil
	}); err != nil {
		log.Fatalln("failed to update:", err)
	}

	// A partial struct can be declared to partially get the data. Here, the
	// color field is dropped.
	var user struct {
		ID   int
		Name string
	}
	var found bool

	if err := userDB.Each("", &user, func([]byte) bool {
		found = user.Name == "want this user"
		return found
	}); err != nil {
		log.Fatalln("failed to iterate:", err)
	}

	if !found {
		log.Fatalln("user not found")
	}

	fmt.Println("Found user with ID", user.ID) // Output: Found user with ID 2
}
