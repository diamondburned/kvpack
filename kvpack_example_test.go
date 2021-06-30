package kvpack_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

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

// TestHideMe put here to hide everything outside example functions.
func TestHideMe(t *testing.T) {}

func ExampleDatabase_Descend() {
	// cleanNamespace will output namespaces similar to regular file paths. It
	// is mostly used for pretty-printing.
	prettyNamespace := func(db *kvpack.Database) string {
		namespace := strings.ReplaceAll(db.Namespace(), kvpack.Separator, "/")
		return strings.TrimPrefix(namespace, kvpack.Namespace)
	}

	db := mustDB("app-name")
	defer db.Close()

	fmt.Println(prettyNamespace(db))

	userDB := db.Descend("users")
	fmt.Println(prettyNamespace(userDB))

	// Output:
	// /app-name
	// /app-name/users
}

func ExampleDatabase_WithNamespace() {
	// cleanNamespace will output namespaces similar to regular file paths. It
	// is mostly used for pretty-printing.
	prettyNamespace := func(db *kvpack.Database) string {
		namespace := strings.ReplaceAll(db.Namespace(), kvpack.Separator, "/")
		return strings.TrimPrefix(namespace, kvpack.Namespace)
	}

	db := mustDB("app-name")
	defer db.Close()

	fmt.Println(prettyNamespace(db))

	userDB := db.WithNamespace("app-name", "users")
	fmt.Println(prettyNamespace(userDB))

	// Reaccess an upper-level namespace.
	topLevelDB := userDB.WithNamespace("app-name")
	fmt.Println(prettyNamespace(topLevelDB))

	// Output:
	// /app-name
	// /app-name/users
	// /app-name
}

func ExampleDatabase_GetFields() {
	type User struct {
		Name  string
		Color string
	}

	db := mustDB("kvpack-demo", "getfields-example")
	defer db.Close()

	users := []User{
		{"diamondburned", "pink"},
		{"somebody else", "cyan"},
		{"anonymous", "red"},
	}

	if err := db.Put([]byte("users"), &users); err != nil {
		log.Fatalln("failed to put user", err)
	}

	// The target can be a string, but it can also be a structure (as in Each's
	// example).
	var color string
	// userDB can be used here with "users." omitted; it is only here as an
	// example.
	if err := db.GetFields("users.0.Color", &color); err != nil {
		log.Fatalln("failed to get user 0:", err)
	}

	fmt.Println("User 0's color is", color) // Output: User 0's color is pink
}

func ExampleTransaction_GetFields() {
	type User struct {
		Name  string
		Color string
	}

	db := mustDB("kvpack-demo", "getfields-example")
	defer db.Close()

	users := []User{
		{"diamondburned", "pink"},
		{"somebody else", "cyan"},
		{"anonymous", "red"},
	}

	if err := db.Update(func(tx *kvpack.Transaction) error {
		return tx.Put([]byte("users"), &users)
	}); err != nil {
		log.Fatalln("failed to put user", err)
	}

	// The target can be a string, but it can also be a structure (as in Each's
	// example).
	var color string
	// userDB can be used here with "users." omitted; it is only here as an
	// example.
	if err := db.View(func(tx *kvpack.Transaction) error {
		return tx.GetFields("users.0.Color", &color)
	}); err != nil {
		log.Fatalln("failed to get user 0:", err)
	}

	fmt.Println("User 0's color is", color) // Output: User 0's color is pink
}

func ExampleDatabase_PutFields() {
	type User struct {
		Name  string
		Color string
	}

	db := mustDB("kvpack-demo", "getfields-example")
	defer db.Close()

	users := []User{
		{"diamondburned", "pink"},
		{"somebody else", "cyan"},
		{"anonymous", "red"},
	}

	if err := db.Put([]byte("users"), &users); err != nil {
		log.Fatalln("failed to put user", err)
	}

	// Override a user's color.
	pink := "pink"
	if err := db.PutFields("users.2.Color", &pink); err != nil {
		log.Fatalln("failed to change color:", err)
	}

	var thirdUser User
	if err := db.GetFields("users.2", &thirdUser); err != nil {
		log.Fatalln("failed to get 3rd user:", err)
	}

	fmt.Println("User", thirdUser.Name, "now has color", thirdUser.Color)

	// Output:
	// User anonymous now has color pink
}

func ExampleTransaction_PutFields() {
	type User struct {
		Name  string
		Color string
	}

	db := mustDB("kvpack-demo", "getfields-example")
	defer db.Close()

	users := []User{
		{"diamondburned", "pink"},
		{"somebody else", "cyan"},
		{"anonymous", "red"},
	}

	if err := db.Update(func(tx *kvpack.Transaction) error {
		return tx.Put([]byte("users"), &users)
	}); err != nil {
		log.Fatalln("failed to put user", err)
	}

	// Override a user's color.
	pink := "pink"

	if err := db.Update(func(tx *kvpack.Transaction) error {
		return tx.PutFields("users.2.Color", &pink)
	}); err != nil {
		log.Fatalln("failed to change color:", err)
	}

	var thirdUser User
	if err := db.GetFields("users.2", &thirdUser); err != nil {
		log.Fatalln("failed to get 3rd user:", err)
	}

	fmt.Println("User", thirdUser.Name, "now has color", thirdUser.Color)

	// Output:
	// User anonymous now has color pink
}

func ExampleDatabase_Each() {
	type User struct {
		ID    int
		Name  string
		Color string
	}

	db := mustDB("kvpack-demo", "each-example")
	defer db.Close()

	userDB := db.Descend("users")

	users := []User{
		{0, "diamondburned", "pink"},
		{1, "somebody else", "cyan"},
		{2, "anonymous", "red"},
	}

	for _, user := range users {
		if err := userDB.Put([]byte(strconv.Itoa(user.ID)), &user); err != nil {
			log.Fatalln("failed to put user", err)
		}
	}

	// A partial struct can be declared to partially get the data. Here, the
	// color field is dropped.
	var user struct {
		Name string
	}
	var fullUser User
	var found bool

	if err := userDB.Each("", &user, func(key []byte) error {
		if user.Name != "diamondburned" {
			return nil
		}

		// Only get the full user once we've scanned for what we want.
		if err := userDB.Get(key, &fullUser); err != nil {
			return errors.Wrap(err, "failed to get the full user")
		}

		found = true
		return kvpack.Break
	}); err != nil {
		log.Fatalln("failed to iterate:", err)
	}

	if !found {
		log.Fatalln("user not found")
	}

	fmt.Printf(
		"Found user %s (ID %d) who has the color %s",
		fullUser.Name, fullUser.ID, fullUser.Color,
	)

	// Output: Found user diamondburned (ID 0) who has the color pink
}

func ExampleTransaction_Each() {
	type User struct {
		ID    int
		Name  string
		Color string
	}

	db := mustDB("kvpack-demo", "each-example")
	defer db.Close()

	userDB := db.Descend("users")

	if err := userDB.Update(func(tx *kvpack.Transaction) error {
		users := []User{
			{0, "diamondburned", "pink"},
			{1, "somebody else", "cyan"},
			{2, "anonymous", "red"},
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
		Name string
	}
	var fullUser User
	var found bool

	if err := userDB.View(func(tx *kvpack.Transaction) error {
		return userDB.Each("", &user, func(key []byte) error {
			if user.Name != "diamondburned" {
				return nil
			}

			// Only get the full user once we've scanned for what we want.
			if err := userDB.Get(key, &fullUser); err != nil {
				return errors.Wrap(err, "failed to get the full user")
			}

			found = true
			return kvpack.Break
		})
	}); err != nil {
		log.Fatalln("failed to iterate:", err)
	}

	if !found {
		log.Fatalln("user not found")
	}

	fmt.Printf(
		"Found user %s (ID %d) who has the color %s",
		fullUser.Name, fullUser.ID, fullUser.Color,
	)

	// Output: Found user diamondburned (ID 0) who has the color pink
}
