package memory_test

import (
	"testing"

	"github.com/ably/ably-server/internal/storage"
	"github.com/ably/ably-server/internal/storage/memory"
	"github.com/ably/ably-server/internal/storage/storagetest"
)

func TestMemoryChannelStoreContract(t *testing.T) {
	storagetest.RunChannelStoreTests(t, func(t *testing.T) storage.Storage {
		return memory.New(memory.Options{})
	})
}
