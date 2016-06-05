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
	lk, ts := getTestLock(200, "{}")
	defer ts.Close()

	locked, err := lk.Lock("mylock", time.Now().Add(10*time.Minute))
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("failed to lock")
	}
}

func TestNoLock(t *testing.T) {
	lk, ts := getTestLock(400,
		`{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}`)
	defer ts.Close()

	locked, err := lk.Lock("mylock", time.Now().Add(10*time.Minute))
	if locked {
		t.Error("Should not have acquired the lock")
	}
	if err != nil {
		t.Error("Expect no error when failing to acquire a lock because it's already locked")
	}
}

func TestLockError(t *testing.T) {
	lk, ts := getTestLock(500, "{}")
	defer ts.Close()
	locked, err := lk.Lock("mylock", time.Now().Add(10*time.Minute))
	if locked {
		t.Error("Should not have acquired the lock")
	}
	if err == nil {
		t.Error("Expect a dynamo based error error.")
	}
}

func TestUnLockSuccess(t *testing.T) {
	lk, ts := getTestLock(200, "{}")
	defer ts.Close()

	err := lk.Unlock("mylock")
	if err != nil {
		t.Error(err)
	}
}

func TestUnLockOwnedByOther(t *testing.T) {
	lk, ts := getTestLock(400,
		`{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}`)
	defer ts.Close()

	err := lk.Unlock("mylock")
	if err == nil {
		t.Error("Expected an error when unlocking a lock we don't own or doesnt' exist.")
	}
}

func TestUnLockFail(t *testing.T) {
	lk, ts := getTestLock(500, "{}")
	defer ts.Close()

	err := lk.Unlock("mylock")
	if err == nil {
		t.Error("Unlock should return an error when the db query fails")
	}
}

func getTestLock(respCode int, respBody string) (*Lock, *httptest.Server) {
	ts, client := getHTTPResponse(respCode, respBody)

	conf := &aws.Config{
		Endpoint:   &ts.URL,
		HTTPClient: client,
		MaxRetries: aws.Int(0),
	}
	db := dynamodb.New(session.New(), conf.WithRegion("us-west-2"))
	return NewLock("testNode12", "locks_table", db), ts
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
