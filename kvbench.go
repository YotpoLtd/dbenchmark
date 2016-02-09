package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"encoding/json"
	"github.com/couchbase/gocb"
	"github.com/gocql/gocql"
	"github.com/satori/go.uuid"
	"strings"
)

type hostList []string

func (hl *hostList) String() string {
	return strings.Join(*hl, ",")
}

func (hl *hostList) Set(value string) error {
	*hl = append(*hl, value)
	return nil
}

var (
	testCouchBase      = flag.Bool("test-couchbase", false, "Decide wether or not to test chouchbase (false)")
	couchBaseUrl       = flag.String("couchbase-url", "couchbase://127.0.0.1", "The url to connect to CouchBase (couchbase://127.0.0.1)")
	couchBaseBucket    = flag.String("couchbase-bucket", "default", "The bucket to use in CouchBase (default)")
	couchBaseTestUsers []*User

	cassandraHosts     = hostList{}
	cassandraTestUsers []*User

	testCassandra       = flag.Bool("test-cassandra", false, "Decide wether or not to test cassandra (false)")
	cassandraPort       = flag.Int("cassandra-port", 9042, "The host on which cassandra runs (9042)")
	cassandraKeyspace   = flag.String("cassandra-keyspace", "benchtest", "The host on which cassandra runs (benchtest)")
	cassandraCQLVersion = flag.String("cassandra-cql-version", "3.2.0", "The CQL version which cassandra uses (3.2.0)")
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
	flag.Var(&cassandraHosts, "cassandra-host", "List of host on which cassandra runs (127.0.0.1)")
	flag.Parse()
	if flag.NFlag() == 0 {
		flag.PrintDefaults()
	}
	if len(cassandraHosts) == 0 {
		cassandraHosts = hostList{"127.0.0.1"}
	}
	rand.Seed(time.Now().UTC().UnixNano())
}

func getUser(i int, randstr string, users *[]*User) *User {
	email := fmt.Sprintf("user%d%s@domain.com", i, randstr)
	id := uuid.NewV5(uuid.NamespaceOID, email).String()
	user := &User{
		Id:        id,
		Email:     uuid.NewV4().String(),
		FirstName: uuid.NewV4().String(),
		LastName:  uuid.NewV4().String(),
	}
	*users = append(*users, user)
	return user
}

func BenchmarkCouchBaseInsertgocb(b *testing.B) {
	var err error
	insertUsers := make([]*User, len(couchBaseTestUsers))
	copy(insertUsers, couchBaseTestUsers)
	db_url, err := url.Parse(*couchBaseUrl)
	mf(err, "parse", b)

	cluster, err := gocb.Connect(db_url.String())
	mf(err, "connect - "+db_url.String(), b)

	bucket, err := cluster.OpenBucket(*couchBaseBucket, "")
	mf(err, "bucket", b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		user := getUser(i, uuid.NewV4().String(), &couchBaseTestUsers)
		_, err := bucket.Insert(user.Id, user, 0)
		mf(err, "Insert", b)
	}
}

func BenchmarkCouchBaseGetgocb(b *testing.B) {
	var err error

	db_url, err := url.Parse(*couchBaseUrl)
	mf(err, "parse", b)

	cluster, err := gocb.Connect(db_url.String())
	mf(err, "connect - "+db_url.String(), b)

	bucket, err := cluster.OpenBucket(*couchBaseBucket, "")
	mf(err, "bucket", b)

	b.ResetTimer()

	max := len(couchBaseTestUsers)
	for i := 0; i < b.N; i++ {
		user := &User{}
		_, err := bucket.Get(couchBaseTestUsers[rand.Intn(max)].Id, *user)
		mf(err, "Get", b)
	}
}

func BenchmarkCassandraInsert(b *testing.B) {
	cluster := getCassandraCluster()
	prepareCassandraCluster(cluster)
	session, err := cluster.CreateSession()
	mf(err, "session", b)
	defer session.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		user := getUser(i, uuid.NewV4().String(), &cassandraTestUsers)
		juser, err := json.Marshal(user)
		mf(err, "json", b)
		err = session.Query("INSERT INTO kvbench (id, value) VALUES (?, ?) IF NOT EXISTS", user.Id, juser).RetryPolicy(nil).Exec()
	}

}

func BenchmarkCassandraGet(b *testing.B) {
	cluster := getCassandraCluster()
	prepareCassandraCluster(cluster)
	session, err := cluster.CreateSession()
	mf(err, "session", b)
	defer session.Close()
	b.ResetTimer()

	max := len(cassandraTestUsers)
	for i := 0; i < b.N; i++ {
		gu := cassandraTestUsers[rand.Intn(max)]
		err = session.Query("SELECT FROM kvbench WHERE id = ?", gu.Id).RetryPolicy(nil).Exec()
		mf(err, "Get", b)
	}
}

func getCassandraCluster() *gocql.ClusterConfig {
	cluster := gocql.NewCluster(cassandraHosts...)
	cluster.CQLVersion = *cassandraCQLVersion
	cluster.Keyspace = *cassandraKeyspace
	cluster.Port = *cassandraPort
	return cluster
}

func prepareCassandraCluster(cluster *gocql.ClusterConfig) {
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	err = session.Query("DROP KEYSPACE IF EXISTS ?", cassandraKeyspace).RetryPolicy(nil).Exec()
	if err != nil {
		panic(err)
	}
	err = session.Query("DROP KEYSPACE IF EXISTS ?", cassandraKeyspace).RetryPolicy(nil).Exec()
	if err != nil {
		panic(err)
	}
	err = session.Query("CREATE KEYSPACE ? WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", cassandraKeyspace).RetryPolicy(nil).Exec()
	if err != nil {
		panic(err)
	}
	err = session.Query("CREATE TABLE ?.kvbench (id UUID primary key, value varchar)", cassandraKeyspace).RetryPolicy(nil).Exec()
	if err != nil {
		panic(err)
	}

}

func main() {
	fmt.Println("Done")
	var benchmarkResults map[string]testing.BenchmarkResult
	benchmarkResults = make(map[string]testing.BenchmarkResult)
	if *testCouchBase {
		benchmarkResults["CouchBaseInsert"] = testing.Benchmark(BenchmarkCouchBaseInsertgocb)
		benchmarkResults["CouchBaseGet"] = testing.Benchmark(BenchmarkCouchBaseGetgocb)
	}

	if *testCassandra {
		benchmarkResults["CassandraInsert"] = testing.Benchmark(BenchmarkCassandraInsert)
		benchmarkResults["CassandraGet"] = testing.Benchmark(BenchmarkCassandraGet)
	}
	for name, bm := range benchmarkResults {
		fmt.Println("name:", name, "bm:", bm)
	}
}
