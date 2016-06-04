package lock

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestLockSuccess(t *testing.T) {
	ts, client := getHTTPResponse(200, "{}")
	defer ts.Close()

	conf := &aws.Config{
		Endpoint:   &ts.URL,
		HTTPClient: client,
	}
	db := dynamodb.New(session.New(), conf.WithRegion("us-west-2"))
	lk := NewLock("t2", "prod.locks", db)
	locked, err := lk.Lock("mylock", time.Now().Add(10*time.Minute))
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("failed to lock")
	}
}

func TestNoLock(t *testing.T) {
	ts, client := getHTTPResponse(400,
		`{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}`)
	defer ts.Close()

	conf := &aws.Config{
		Endpoint:   &ts.URL,
		HTTPClient: client,
	}
	db := dynamodb.New(session.New(), conf.WithRegion("us-west-2"))
	lk := NewLock("t2", "prod.locks", db)
	locked, err := lk.Lock("mylock", time.Now().Add(10*time.Minute))
	if locked {
		t.Error("Should not have acquired the lock")
	}
	if err != nil {
		t.Error("Expect no error when failing to acquire a lock because it's already locked")
	}

}

func getHTTPResponse(code int, body string) (*httptest.Server, *http.Client) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(code)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, body)
	}))

	transport := &http.Transport{
		Proxy: func(req *http.Request) (*url.URL, error) {
			return url.Parse(server.URL)
		},
	}

	return server, &http.Client{Transport: transport}
}
