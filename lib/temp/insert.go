package main

import (
	"encoding/base64"
	"fmt"
	"github.com/pborman/uuid"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/gravitational/teleport/lib/defaults"
	"github.com/gravitational/teleport/lib/services"
)

func main() {

	for i := 0; i < 500; i++ {
		fmt.Printf("--> %v\n", i)
		id := uuid.NewUUID()

		server := &services.ServerV2{
			Kind:    services.KindNode,
			Version: services.V2,
			Metadata: services.Metadata{
				Name:      id.String(),
				Namespace: defaults.Namespace,
			},
			Spec: services.ServerSpecV2{
				Addr:     "127.0.0.1:3022",
				Hostname: uuid.NewUUID().String(),
			},
		}
		resource, err := services.GetServerMarshaler().MarshalServer(server)
		if err != nil {
			log.Fatal(err)
			return
		}
		encodedResource := base64.StdEncoding.EncodeToString([]byte(resource))

		path := "http://127.0.0.1:2379/v2/keys/teleport-one.secrets/namespaces/default/nodes/" + id.String()
		body := strings.NewReader("value=" + encodedResource)

		req, err := http.NewRequest("PUT", path, body)
		if err != nil {
			log.Fatal(err)
			return
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != 201 {
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatal(err)
				return
			}
			fmt.Printf("%v\n", string(b))
		}
	}
}
