package dbenchmark

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"testing"
        "math/rand"
	"time"

	"github.com/couchbase/gocb"
	//"github.com/couchbase/go-couchbase"
	"github.com/satori/go.uuid"
)

var couchBaseUrl = flag.String("couchbase-url", "couchbase://127.0.0.1", "The url to connect to CouchBase")
var couchBaseBucket = flag.String("couchbase-bucket", "default", "The bucket to use in CouchBase")

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
        rand.Seed(time.Now().UTC().UnixNano())
}
func getUser(i int) *User{
        email := fmt.Sprintf("user%d%s@domain.com", i, uuid.NewV4().String())
        id := uuid.NewV5(uuid.NamespaceOID, email).String()
	return &User{
                Id:        id,
                Email:	   email,
                FirstName: uuid.NewV4().String(),
                LastName:  uuid.NewV4().String(),
        }
}
//func BenchmarkCouchBasegocouchbase(b *testing.B) {
//	var err error
//
//	db_url, err := url.Parse(*couchBaseUrl)
//	mf(err, "parse", b)
//
//
//	log.Println("Connecting To Couchbase: ", db_url.String())
//	cluster, err := couchbase.Connect(db_url.String())
//	mf(err, "connect - "+db_url.String(), b)
//
//	pool, err := cluster.GetPool("default")
//	mf(err, "pool", b)
//
//	log.Println("Connecting To Bucket: ", *couchBaseBucket)
//	bucket, err := pool.GetBucket(*couchBaseBucket)
//	mf(err, "bucket", b)
//
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		added, err := bucket.Add(users[i].Id, 0, users[i])
//		mf(err, "add", b)
//		if !added {
//			log.Printf("couldn't add user %v", users[i])
//		}
//	}
//}

func BenchmarkCouchBasegocb(b *testing.B) {
	var err error

	db_url, err := url.Parse(*couchBaseUrl)
	mf(err, "parse", b)

	cluster, err := gocb.Connect(db_url.String())
	mf(err, "connect - "+db_url.String(), b)

	bucket, err := cluster.OpenBucket(*couchBaseBucket, "")
	mf(err, "bucket", b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		user := getUser(i)
		_, err := bucket.Insert(user.Id, user, 0)
		mf(err, "Insert", b)
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

