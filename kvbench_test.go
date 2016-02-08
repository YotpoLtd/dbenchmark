package dbenchmark

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/couchbase/gocb"
	"github.com/satori/go.uuid"
)

var (
	couchBaseUrl    = flag.String("couchbase-url", "couchbase://127.0.0.1", "The url to connect to CouchBase")
	couchBaseBucket = flag.String("couchbase-bucket", "default", "The bucket to use in CouchBase")
	users []*User
)

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
func getUser(i int, randstr string) *User {
	email := fmt.Sprintf("user%d%s@domain.com", i, randstr)
	id := uuid.NewV5(uuid.NamespaceOID, email).String()
	user := &User{
		Id:        id,
		Email:     email,
		FirstName: uuid.NewV4().String(),
		LastName:  uuid.NewV4().String(),
	}
	users = append(users, user)
	return user
}

func BenchmarkCouchBaseInsertgocb1(b *testing.B) {
	var err error
	insertUsers := make([]*User, len(users))
	copy(insertUsers, users)
	db_url, err := url.Parse(*couchBaseUrl)
	mf(err, "parse", b)

	cluster, err := gocb.Connect(db_url.String())
	mf(err, "connect - "+db_url.String(), b)

	bucket, err := cluster.OpenBucket(*couchBaseBucket, "")
	mf(err, "bucket", b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		user := getUser(i, uuid.NewV4().String())
		_, err := bucket.Insert(user.Id, user, 0)
		mf(err, "Insert", b)
	}
}


func BenchmarkCouchBaseGetgocb2(b *testing.B) {
	var err error

	db_url, err := url.Parse(*couchBaseUrl)
	mf(err, "parse", b)

	cluster, err := gocb.Connect(db_url.String())
	mf(err, "connect - "+db_url.String(), b)

	bucket, err := cluster.OpenBucket(*couchBaseBucket, "")
	mf(err, "bucket", b)

	b.ResetTimer()

	max := len(users)
	for i := 0; i < b.N; i++ {
		user := &User{}
		_, err := bucket.Get(users[rand.Intn(max)].Id, *user)
		mf(err, "Get", b)
	}
}
