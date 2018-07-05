/*
Copyright 2016-2017 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dir

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/utils"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
)

var _ = fmt.Printf

const (
	defaultDirMode  os.FileMode = 0770
	defaultFileMode os.FileMode = 0600

	// name of this backend type (as seen in 'storage/type' in YAML)
	backendName = "dir"

	// selfLock is the lock used internally for compare-and-swap
	selfLock = ".backend"

	// subdirectory where locks are stored
	locksBucket = ".locks"

	// reservedPrefix is a character which bucket/key names cannot begin with
	reservedPrefix = '.'
)

// Backend implements backend.Backend interface using a regular
// POSIX-style filesystem
type Backend struct {
	// RootDir is the root (home) directory where the backend
	// stores all the data.
	RootDir string

	// InternalClock is a test-friendly source of current time
	InternalClock clockwork.Clock

	// log is a structured component logger.
	log *logrus.Entry
}

// Clock returns the clock used by this backend.
func (b *Backend) Clock() clockwork.Clock {
	return b.InternalClock
}

// GetName returns the name of this backend.
func GetName() string {
	return backendName
}

type bucketValue struct {
	Value      []byte    `json:"value"`
	ExpiryTime time.Time `json:"expiry,omitempty"`
}

// New creates a new instance of Filesystem backend, it conforms to backend.NewFunc API
func New(params backend.Params) (backend.Backend, error) {
	rootDir := params.GetString("path")
	if rootDir == "" {
		rootDir = params.GetString("data_dir")
	}
	if rootDir == "" {
		return nil, trace.BadParameter("filesystem backend: 'path' is not set")
	}

	bk := &Backend{
		RootDir:       rootDir,
		InternalClock: clockwork.NewRealClock(),
		log: logrus.WithFields(logrus.Fields{
			trace.Component: "backend:dir",
			trace.ComponentFields: logrus.Fields{
				"dir": rootDir,
			},
		}),
	}

	//locksDir := path.Join(bk.RootDir, locksBucket)
	//if err := os.MkdirAll(locksDir, defaultDirMode); err != nil {
	//	return nil, trace.ConvertSystemError(err)
	//}

	// Wrap the backend in a input sanitizer and return it.
	return backend.NewSanitizer(bk), nil
}

func (bk *Backend) flatten(bucket []string) string {
	p := strings.Join(bucket, "/")
	return bk.RootDir + "/" + url.QueryEscape(p)
}

// GetItems is a function that returns keys in batch
func (bk *Backend) GetItems(bucket []string) ([]backend.Item, error) {
	var out []backend.Item

	// Get a list of all buckets in the backend.
	files, err := ioutil.ReadDir(path.Join(bk.RootDir))
	if err != nil {
		return nil, trace.ConvertSystemError(err)
	}

	// Loop over all buckets.
	for _, fi := range files {
		// Skip over any buckets without a matching prefix.
		if !strings.HasPrefix(bk.RootDir+"/"+fi.Name(), bk.flatten(bucket)) {
			continue
		}

		// The bucket matches the prefix.
		f, err := os.OpenFile(bk.RootDir+"/"+fi.Name(), os.O_RDONLY, defaultFileMode)
		if err != nil {
			return nil, trace.ConvertSystemError(err)
		}
		defer f.Close()

		// Lock the file so no one else can access it.
		if err := utils.FSReadLock(f); err != nil {
			return nil, trace.Wrap(err)
		}
		defer utils.FSUnlock(f)

		// Read the file into a map of key/value pairs.
		var items map[string]bucketValue
		bytes, err := ioutil.ReadAll(f)
		if err != nil {
			return nil, trace.ConvertSystemError(err)
		}
		err = json.Unmarshal(bytes, &items)
		if err != nil {
			return nil, trace.Wrap(err)
		}

		// Flatten all keys and return them to the caller.
		for k, v := range items {
			fff := bk.RootDir + "/" + fi.Name()
			if fff != bk.flatten(bucket) {
				s, err := suffix(fff, bk.flatten(bucket))
				if err != nil {
					return nil, trace.Wrap(err)
				}

				if bk.isExpired(v) {
					//bk.DeleteKey(bucket, key)
					continue
				}

				out = append(out, backend.Item{
					Key:   s,
					Value: v.Value,
				})

				continue
			}

			if bk.isExpired(v) {
				continue
			}

			out = append(out, backend.Item{
				Key:   k,
				Value: v.Value,
			})
		}
	}

	// Sort and return results.
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})

	return out, nil
}

func suffix(fullpath string, prefix string) (string, error) {
	df, err := url.QueryUnescape(fullpath)
	if err != nil {
		return "", trace.Wrap(err)
	}
	dp, err := url.QueryUnescape(prefix)
	if err != nil {
		return "", trace.Wrap(err)
	}

	remain := df[len(dp)+1:]
	vals := strings.Split(remain, "/")
	return vals[0], nil
}

func isEmpty(f *os.File) (bool, error) {
	fi, err := f.Stat()
	if err != nil {
		return false, trace.Wrap(err)
	}

	if fi.Size() > 0 {
		return false, nil
	}

	return true, nil
}

func (bk *Backend) UpsertItems(bucket []string, newItems []backend.Item) error {
	// The bucket matches the prefix.
	bkt := bk.flatten(bucket)
	f, err := os.OpenFile(bkt, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		return trace.ConvertSystemError(err)
	}
	defer f.Close()

	// Lock the bucket so no other go routines can access it.
	if err := utils.FSWriteLock(f); err != nil {
		return trace.Wrap(err)
	}
	defer utils.FSUnlock(f)

	var items map[string]bucketValue

	// Read the file into a map of key/value pairs.
	ok, err := isEmpty(f)
	if err != nil {
		return trace.Wrap(err)
	}
	if !ok {
		// Read bucket in.
		bytes, err := ioutil.ReadAll(f)
		if err != nil {
			return trace.ConvertSystemError(err)
		}

		// Truncate the file
		if _, err := f.Seek(0, 0); err != nil {
			return trace.ConvertSystemError(err)
		}
		if err := f.Truncate(0); err != nil {
			return trace.ConvertSystemError(err)
		}

		err = json.Unmarshal(bytes, &items)
		if err != nil {
			return trace.Wrap(err)
		}

	} else {
		items = make(map[string]bucketValue)
	}

	for _, e := range newItems {
		items[e.Key] = bk.bucketValueWithTTL(e.Value, e.TTL)
	}

	// Marshal and write file back out.
	bytes, err := json.Marshal(items)
	if err != nil {
		return trace.Wrap(err)
	}
	n, err := f.Write(bytes)
	if err == nil && n < len(bytes) {
		return trace.Wrap(io.ErrShortWrite)
	}

	return nil

}

func (bk *Backend) UpsertVal(bucket []string, key string, val []byte, ttl time.Duration) error {
	// The bucket matches the prefix.
	bkt := bk.flatten(bucket)
	f, err := os.OpenFile(bkt, os.O_CREATE|os.O_RDWR, defaultFileMode)
	if err != nil {
		fmt.Printf("--> UpsertVal: err=%v\n", err)
		return trace.ConvertSystemError(err)
	}
	defer f.Close()

	// Lock the bucket so no other go routines can access it.
	if err := utils.FSWriteLock(f); err != nil {
		return trace.Wrap(err)
	}
	defer utils.FSUnlock(f)

	var items map[string]bucketValue

	// Read the file into a map of key/value pairs.
	ok, err := isEmpty(f)
	if err != nil {
		return trace.Wrap(err)
	}
	if !ok {
		// Read bucket in.
		bytes, err := ioutil.ReadAll(f)
		if err != nil {
			return trace.ConvertSystemError(err)
		}

		// Truncate the file
		if _, err := f.Seek(0, 0); err != nil {
			return trace.ConvertSystemError(err)
		}
		if err := f.Truncate(0); err != nil {
			return trace.ConvertSystemError(err)
		}

		err = json.Unmarshal(bytes, &items)
		if err != nil {
			return trace.Wrap(err)
		}

	} else {
		items = make(map[string]bucketValue)
	}

	// Update the value in the map.
	items[key] = bk.bucketValueWithTTL(val, ttl)

	// Marshal and write file back out.
	bytes, err := json.Marshal(items)
	if err != nil {
		return trace.Wrap(err)
	}
	n, err := f.Write(bytes)
	if err == nil && n < len(bytes) {
		return trace.Wrap(io.ErrShortWrite)
	}

	return nil
}

// GetKeys returns a list of keys for a given path
func (bk *Backend) GetKeys(bucket []string) ([]string, error) {
	items, err := bk.GetItems(bucket)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	keys := make([]string, len(items))
	for i, e := range items {
		keys[i] = e.Key
	}
	return keys, nil

	//files, err := ioutil.ReadDir(path.Join(bk.RootDir, path.Join(bucket...)))
	//if err != nil {
	//	if os.IsNotExist(err) {
	//		return []string{}, nil
	//	}
	//	return nil, trace.ConvertSystemError(err)
	//}
	//// enumerate all directory entries and select only non-hidden files
	//retval := make([]string, 0)
	//for _, fi := range files {
	//	name := fi.Name()
	//	// legal keys cannot start with '.' (resrved prefix)
	//	if name[0] != reservedPrefix {
	//		retval = append(retval, name)
	//	}
	//}
	//return retval, nil
}

// CreateVal creates value with a given TTL and key in the bucket
// if the value already exists, returns AlreadyExistsError
func (bk *Backend) CreateVal(bucket []string, key string, val []byte, ttl time.Duration) error {
	bkt := bk.flatten(bucket)
	f, err := os.OpenFile(bkt, os.O_RDWR|os.O_CREATE, defaultFileMode)
	if err != nil {
		//if os.IsExist(err) {
		//	return trace.AlreadyExists("%s/%s already exists", bkt, key)
		//}
		return trace.ConvertSystemError(err)
	}
	defer f.Close()

	// Lock the bucket so no other go routines can access it.
	if err := utils.FSWriteLock(f); err != nil {
		return trace.Wrap(err)
	}
	defer utils.FSUnlock(f)

	var items map[string]bucketValue

	// Read the file into a map of key/value pairs.
	ok, err := isEmpty(f)
	if err != nil {
		return trace.Wrap(err)
	}
	if !ok {
		// Read bucket in.
		bytes, err := ioutil.ReadAll(f)
		if err != nil {
			return trace.ConvertSystemError(err)
		}

		err = json.Unmarshal(bytes, &items)
		if err != nil {
			return trace.Wrap(err)
		}
	} else {
		items = make(map[string]bucketValue)
	}

	// If the key exists, return a trace.AlreadyExists
	_, ok = items[key]
	if ok {
		return trace.AlreadyExists("file already exists")
	}

	// Update the value in the map.
	items[key] = bk.bucketValueWithTTL(val, ttl)

	// Marshal and write file back out.
	bytes, err := json.Marshal(items)
	if err != nil {
		return trace.Wrap(err)
	}
	// Truncate the file
	if _, err := f.Seek(0, 0); err != nil {
		return trace.ConvertSystemError(err)
	}
	if err := f.Truncate(0); err != nil {
		return trace.ConvertSystemError(err)
	}
	n, err := f.Write(bytes)
	if err == nil && n < len(bytes) {
		return trace.Wrap(io.ErrShortWrite)
	}

	return nil

}

func (bk *Backend) bucketValueWithTTL(value []byte, ttl time.Duration) bucketValue {
	bv := bucketValue{Value: value}
	if ttl == backend.Forever {
		return bv
	}
	bv.ExpiryTime = bk.Clock().Now().Add(ttl)
	return bv

}

func (bk *Backend) isExpired(bv bucketValue) bool {
	if bv.ExpiryTime.IsZero() {
		return false
	}
	return bk.Clock().Now().After(bv.ExpiryTime)
}

//// CreateVal creates value with a given TTL and key in the bucket
//// if the value already exists, returns AlreadyExistsError
//func (bk *Backend) CreateVal(bucket []string, key string, val []byte, ttl time.Duration) error {
//	// do not allow keys that start with a dot
//	if key[0] == reservedPrefix {
//		return trace.BadParameter("invalid key: '%s'. Key names cannot start with '.'", key)
//	}
//	// create the directory:
//	dirPath := path.Join(bk.RootDir, path.Join(bucket...))
//	err := os.MkdirAll(dirPath, defaultDirMode)
//	if err != nil {
//		return trace.ConvertSystemError(err)
//	}
//	// create the file (AKA "key"):
//	filename := path.Join(dirPath, key)
//	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, defaultFileMode)
//	if err != nil {
//		if os.IsExist(err) {
//			return trace.AlreadyExists("%s/%s already exists", dirPath, key)
//		}
//		return trace.ConvertSystemError(err)
//	}
//	defer f.Close()
//	if err := utils.FSWriteLock(f); err != nil {
//		return trace.Wrap(err)
//	}
//	defer utils.FSUnlock(f)
//	if err := f.Truncate(0); err != nil {
//		return trace.ConvertSystemError(err)
//	}
//	n, err := f.Write(val)
//	if err == nil && n < len(val) {
//		return trace.Wrap(io.ErrShortWrite)
//	}
//	return trace.Wrap(bk.applyTTL(dirPath, key, ttl))
//}

// CompareAndSwapVal compares and swap values in atomic operation
func (bk *Backend) CompareAndSwapVal(bucket []string, key string, val []byte, prevVal []byte, ttl time.Duration) error {
	// The bucket matches the prefix.
	bkt := bk.flatten(bucket)
	f, err := os.OpenFile(bkt, os.O_RDWR, defaultFileMode)
	if er := trace.ConvertSystemError(err); err != nil {
		if trace.IsNotFound(er) {
			return trace.CompareFailed("%v/%v did not match expected value", bucket, key)
		}
		return trace.Wrap(er)
	}
	defer f.Close()

	// Lock the bucket so no other go routines can access it.
	if err := utils.FSWriteLock(f); err != nil {
		return trace.Wrap(err)
	}
	defer utils.FSUnlock(f)

	var items map[string]bucketValue

	// Read the file into a map of key/value pairs.
	ok, err := isEmpty(f)
	if err != nil {
		return trace.Wrap(err)
	}
	if !ok {
		// Read bucket in.
		bytes, err := ioutil.ReadAll(f)
		if err != nil {
			return trace.ConvertSystemError(err)
		}

		err = json.Unmarshal(bytes, &items)
		if err != nil {
			return trace.Wrap(err)
		}

	} else {
		items = make(map[string]bucketValue)
	}

	oldVal, ok := items[key]
	if !ok {
		return trace.CompareFailed("%v/%v did not match expected value", bkt, key)
	}

	if bytes.Compare(oldVal.Value, prevVal) != 0 {
		return trace.CompareFailed("%v/%v did not match expected value", bkt, key)
	}

	// Update the value in the map.
	items[key] = bk.bucketValueWithTTL(val, ttl)

	// Marshal and write file back out.
	bytes, err := json.Marshal(items)
	if err != nil {
		return trace.Wrap(err)
	}

	// Truncate the file
	if _, err := f.Seek(0, 0); err != nil {
		return trace.ConvertSystemError(err)
	}
	if err := f.Truncate(0); err != nil {
		return trace.ConvertSystemError(err)
	}
	n, err := f.Write(bytes)
	if err == nil && n < len(bytes) {
		return trace.Wrap(io.ErrShortWrite)
	}

	return nil

}

//// CompareAndSwapVal compares and swap values in atomic operation
//func (bk *Backend) CompareAndSwapVal(bucket []string, key string, val []byte, prevVal []byte, ttl time.Duration) error {
//	if len(prevVal) == 0 {
//		return trace.BadParameter("missing prevVal parameter, to atomically create item, use CreateVal method")
//	}
//	// do not allow keys that start with a dot
//	if key[0] == reservedPrefix {
//		return trace.BadParameter("invalid key: '%s'. Key names cannot start with '.'", key)
//	}
//	// create the directory:
//	dirPath := path.Join(bk.RootDir, path.Join(bucket...))
//	err := os.MkdirAll(dirPath, defaultDirMode)
//	if err != nil {
//		return trace.ConvertSystemError(err)
//	}
//	// create the file (AKA "key"):
//	filename := path.Join(dirPath, key)
//	f, err := os.OpenFile(filename, os.O_RDWR, defaultFileMode)
//	if err != nil {
//		err = trace.ConvertSystemError(err)
//		if trace.IsNotFound(err) {
//			return trace.CompareFailed("%v/%v did not match expected value", dirPath, key)
//		}
//		return trace.Wrap(err)
//	}
//	defer f.Close()
//	if err := utils.FSWriteLock(f); err != nil {
//		return trace.Wrap(err)
//	}
//	defer utils.FSUnlock(f)
//	// before writing, make sure the values are equal
//	oldVal, err := ioutil.ReadAll(f)
//	if err != nil {
//		return trace.ConvertSystemError(err)
//	}
//	if bytes.Compare(oldVal, prevVal) != 0 {
//		return trace.CompareFailed("%v/%v did not match expected value", dirPath, key)
//	}
//	if _, err := f.Seek(0, 0); err != nil {
//		return trace.ConvertSystemError(err)
//	}
//	if err := f.Truncate(0); err != nil {
//		return trace.ConvertSystemError(err)
//	}
//	n, err := f.Write(val)
//	if err == nil && n < len(val) {
//		return trace.Wrap(io.ErrShortWrite)
//	}
//	return trace.Wrap(bk.applyTTL(dirPath, key, ttl))
//}

// GetVal return a value for a given key in the bucket
func (bk *Backend) GetVal(bucket []string, key string) ([]byte, error) {
	// The bucket matches the prefix.
	bkt := bk.flatten(bucket)
	f, err := os.OpenFile(bkt, os.O_RDWR, defaultFileMode)
	if err != nil {
		// Get a list of all buckets in the backend.
		files, er := ioutil.ReadDir(path.Join(bk.RootDir))
		if er != nil {
			return nil, trace.ConvertSystemError(err)
		}
		var matched int
		for _, fi := range files {
			// Skip over any buckets without a matching prefix.
			if strings.HasPrefix(bk.RootDir+"/"+fi.Name(), bk.flatten(bucket)+"%2F"+key) {
				matched = matched + 1
			}
		}
		if matched > 0 {
			return nil, trace.BadParameter("%q is not a valid key", key)
		}
		return nil, trace.ConvertSystemError(err)
	}
	defer f.Close()

	// Lock the file so no one else can access it.
	if err := utils.FSWriteLock(f); err != nil {
		return nil, trace.Wrap(err)
	}
	defer utils.FSUnlock(f)

	// Read bucket in.
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, trace.ConvertSystemError(err)
	}

	// this could happen when CreateKey or UpsertKey created a file
	// but, GetVal managed to get readLock right after it,
	// so there are no contents there
	if len(bytes) == 0 {
		return nil, trace.NotFound("key %q is not found", key)
	}

	var items map[string]bucketValue

	err = json.Unmarshal(bytes, &items)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	val, ok := items[key]
	if !ok {
		return nil, trace.NotFound("key %q is not found", key)
	}

	if bk.isExpired(val) {
		delete(items, key)

		// Marshal and write file back out.
		bytes, err := json.Marshal(items)
		if err != nil {
			return nil, trace.Wrap(err)
		}

		// Truncate the file
		if _, err := f.Seek(0, 0); err != nil {
			return nil, trace.ConvertSystemError(err)
		}
		if err := f.Truncate(0); err != nil {
			return nil, trace.ConvertSystemError(err)
		}
		n, err := f.Write(bytes)
		if err == nil && n < len(bytes) {
			return nil, trace.Wrap(io.ErrShortWrite)
		}

		return nil, trace.NotFound("key %q is not found", key)
	}

	return val.Value, nil
}

/*
// UpsertVal updates or inserts value with a given TTL into a bucket
// ForeverTTL for no TTL
func (bk *Backend) UpsertVal(bucket []string, key string, val []byte, ttl time.Duration) error {
	// create the directory:
	dirPath := path.Join(bk.RootDir, path.Join(bucket...))
	err := os.MkdirAll(dirPath, defaultDirMode)
	if err != nil {
		return trace.Wrap(err)
	}
	filename := path.Join(dirPath, key)
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, defaultFileMode)
	if err != nil {
		if os.IsExist(err) {
			return trace.AlreadyExists("%s/%s already exists", dirPath, key)
		}
		return trace.ConvertSystemError(err)
	}
	defer f.Close()
	if err := utils.FSWriteLock(f); err != nil {
		return trace.Wrap(err)
	}
	defer utils.FSUnlock(f)
	if err := f.Truncate(0); err != nil {
		return trace.ConvertSystemError(err)
	}
	n, err := f.Write(val)
	if err == nil && n < len(val) {
		return trace.Wrap(io.ErrShortWrite)
	}
	return trace.Wrap(bk.applyTTL(dirPath, key, ttl))
}

// GetVal return a value for a given key in the bucket
func (bk *Backend) GetVal(bucket []string, key string) ([]byte, error) {
	dirPath := path.Join(path.Join(bk.RootDir, path.Join(bucket...)))
	filename := path.Join(dirPath, key)
	expired, err := bk.checkTTL(dirPath, key)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if expired {
		bk.DeleteKey(bucket, key)
		return nil, trace.NotFound("key %q is not found", key)
	}
	f, err := os.OpenFile(filename, os.O_RDONLY, defaultFileMode)
	if err != nil {
		// GetVal() on a bucket must return 'BadParameter' error:
		if fi, _ := os.Stat(filename); fi != nil && fi.IsDir() {
			return nil, trace.BadParameter("%q is not a valid key", key)
		}
		return nil, trace.ConvertSystemError(err)
	}
	defer f.Close()
	if err := utils.FSReadLock(f); err != nil {
		return nil, trace.Wrap(err)
	}
	defer utils.FSUnlock(f)
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, trace.ConvertSystemError(err)
	}
	// this could happen when CreateKey or UpsertKey created a file
	// but, GetVal managed to get readLock right after it,
	// so there are no contents there
	if len(bytes) == 0 {
		return nil, trace.NotFound("key %q is not found", key)
	}
	return bytes, nil
}
*/

// DeleteKey deletes a key in a bucket
func (bk *Backend) DeleteKey(bucket []string, key string) error {
	// The bucket matches the prefix.
	bkt := bk.flatten(bucket)
	f, err := os.OpenFile(bkt, os.O_RDWR, defaultFileMode)
	if err != nil {
		return trace.ConvertSystemError(err)
	}
	defer f.Close()

	// Lock the bucket so no other go routines can access it.
	if err := utils.FSWriteLock(f); err != nil {
		return trace.Wrap(err)
	}
	defer utils.FSUnlock(f)

	var items map[string]bucketValue

	// Read the file into a map of key/value pairs.
	ok, err := isEmpty(f)
	if err != nil {
		return trace.NotFound("key not found")
	}
	if !ok {
		// Read bucket in.
		bytes, err := ioutil.ReadAll(f)
		if err != nil {
			return trace.ConvertSystemError(err)
		}

		err = json.Unmarshal(bytes, &items)
		if err != nil {
			return trace.Wrap(err)
		}

	} else {
		items = make(map[string]bucketValue)
	}

	_, ok = items[key]
	if !ok {
		return trace.NotFound("key not found")
	}

	delete(items, key)

	// Marshal and write file back out.
	bytes, err := json.Marshal(items)
	if err != nil {
		return trace.Wrap(err)
	}

	// Truncate the file
	if _, err := f.Seek(0, 0); err != nil {
		return trace.ConvertSystemError(err)
	}
	if err := f.Truncate(0); err != nil {
		return trace.ConvertSystemError(err)
	}
	n, err := f.Write(bytes)
	if err == nil && n < len(bytes) {
		return trace.Wrap(io.ErrShortWrite)
	}

	return nil

	//dirPath := path.Join(bk.RootDir, path.Join(bucket...))
	//filename := path.Join(dirPath, key)
	//f, err := os.OpenFile(filename, os.O_RDONLY, defaultFileMode)
	//if err != nil {
	//	if fi, _ := os.Stat(filename); fi != nil && fi.IsDir() {
	//		return trace.BadParameter("%q is not a valid key", key)
	//	}
	//	return trace.ConvertSystemError(err)
	//}
	//defer f.Close()
	//if err := utils.FSWriteLock(f); err != nil {
	//	return trace.Wrap(err)
	//}
	//defer utils.FSUnlock(f)
	//if err := os.Remove(bk.ttlFile(dirPath, key)); err != nil {
	//	if !os.IsNotExist(err) {
	//		bk.log.Warn(err)
	//	}
	//}
	//return trace.ConvertSystemError(os.Remove(filename))
}

// DeleteBucket deletes the bucket by a given path.
func (bk *Backend) DeleteBucket(parent []string, bucket string) error {
	file := bk.flatten(parent) + "%2F" + bucket

	err := os.Remove(file)
	if err != nil {
		return trace.ConvertSystemError(err)
	}

	return nil
}

// AcquireLock grabs a lock that will be released automatically in TTL
func (bk *Backend) AcquireLock(token string, ttl time.Duration) (err error) {
	bk.log.Debugf("AcquireLock(%s)", token)

	if err = backend.ValidateLockTTL(ttl); err != nil {
		return trace.Wrap(err)
	}

	bucket := []string{locksBucket}
	for {
		// GetVal will clear TTL on a lock
		bk.GetVal(bucket, token)

		// CreateVal is atomic:
		err = bk.CreateVal(bucket, token, []byte{1}, ttl)
		if err == nil {
			break // success
		}
		if trace.IsAlreadyExists(err) { // locked? wait and repeat:
			bk.Clock().Sleep(250 * time.Millisecond)
			continue
		}
		return trace.ConvertSystemError(err)
	}

	return nil
}

// ReleaseLock forces lock release before TTL
func (bk *Backend) ReleaseLock(token string) (err error) {
	bk.log.Debugf("ReleaseLock(%s)", token)

	if err = bk.DeleteKey([]string{locksBucket}, token); err != nil {
		if !os.IsNotExist(err) {
			return trace.ConvertSystemError(err)
		}
	}
	return nil
}

// Close releases the resources taken up by a backend
func (bk *Backend) Close() error {
	return nil
}
