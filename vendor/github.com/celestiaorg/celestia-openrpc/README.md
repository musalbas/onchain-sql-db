# celestia-openrpc

`celestia-openrpc` is a golang client for [celestia-node RPC](https://docs.celestia.org/developers/node-api/), without depenencies on celestia-node/celestia-app/cosmos-sdk.

This client library is useful for environments where dependencies on celestia-node/celestia-app/cosmos-sdk are not possible or desired.

## Examples

For a full tutorial on how to use this library, visit the official [guide](https://docs.celestia.org/developers/golang-client-tutorial).

For more examples, see the [examples](./examples) directory.

### Create a new client and submit and fetch a blob

```go
import (
	"bytes"
	"context"
	"fmt"

	client "github.com/celestiaorg/celestia-openrpc"
	"github.com/celestiaorg/celestia-openrpc/types/blob"
	"github.com/celestiaorg/celestia-openrpc/types/share"
	"github.com/celestiaorg/rsmt2d"
)

func main() {
	SubmitBlob(context.Background(), "ws://localhost:26658", "JWT_TOKEN")
}

// SubmitBlob submits a blob containing "Hello, World!" to the 0xDEADBEEF namespace. It uses the default signer on the running node.
func SubmitBlob(ctx context.Context, url string, token string) error {
	client, err := client.NewClient(ctx, url, token)
	if err != nil {
		return err
	}

	// let's post to 0xDEADBEEF namespace
	namespace, err := share.NewBlobNamespaceV0([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	if err != nil {
		return err
	}

	// create a blob
	helloWorldBlob, err := blob.NewBlobV0(namespace, []byte("Hello, World!"))
	if err != nil {
		return err
	}

	// submit the blob to the network
	height, err := client.Blob.Submit(ctx, []*blob.Blob{helloWorldBlob}, blob.DefaultGasPrice())
	if err != nil {
		return err
	}

	fmt.Printf("Blob was included at height %d\n", height)

	// fetch the blob back from the network
	retrievedBlobs, err := client.Blob.GetAll(ctx, height, []share.Namespace{namespace})
	if err != nil {
		return err
	}

	fmt.Printf("Blobs are equal? %v\n", bytes.Equal(helloWorldBlob.Commitment, retrievedBlobs[0].Commitment))
	return nil
}
```
