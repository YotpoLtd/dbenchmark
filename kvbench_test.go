package dbenchmark

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"testing"

	"github.com/couchbase/go-couchbase"
	"github.com/satori/go.uuid"
)

var couchBaseUrl = flag.String("couchbase-url", "http://127.0.0.1:8091", "The url to connect to CouchBase")
var couchBaseBucket = flag.String("couchbase-bucket", "default", "The bucket to use in CouchBase")
var users []*User

type User struct {
	Id        string `json:"id"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func mf(err error, msg string, b *testing.B) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
		b.Fatal(err)
	}
}

func init() {
	flag.Parse()
	for i := 0; i < 1000000; i++ {
		email := fmt.Sprintf("user%d@domain.com", i)
		id := uuid.NewV5(uuid.NamespaceX500, email).String()
		user := &User{
			Id:        id,
			FirstName: uuid.NewV4().String(),
			LastName:  uuid.NewV4().String(),
		}
		users = append(users, user)
	}

}

func BenchmarkCouchBase(b *testing.B) {
	var err error

	db_url, err := url.Parse(*couchBaseUrl)
	mf(err, "parse", b)

	connection, err := couchbase.Connect(db_url.String())
	mf(err, "connect - "+db_url.String(), b)

	pool, err := connection.GetPool("default")
	mf(err, "pool", b)

	bucket, err := pool.GetBucket(*couchBaseBucket)
	mf(err, "bucket", b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		added, err := bucket.Add(users[i].Id, 0, users[i])
		mf(err, "Add", b)
		if !added {
			log.Fatalf("A Document with the same id of (%s) already exists.\n", users[i].Id)
		}
	}
}

//
//func BenchmarkCassandra(b *testing.B) {
//	var err error
//	//Connect to db
//	if err != nil {
//		b.Fatal(err)
//	}
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//
//	}
//}
//
//func BenchmarkSkylla(b *testing.B) {
//	var err error
//	//Connect to db
//	if err != nil {
//		b.Fatal(err)
//	}
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//
//	}
//}
