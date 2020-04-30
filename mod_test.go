package noise_test

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/R-Niagra/noise-1"
	"github.com/oasislabs/ed25519"
	"github.com/stretchr/testify/assert"
)

func TestMarshalJSON(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)

	var pubKey noise.PublicKey
	var privKey noise.PrivateKey

	copy(pubKey[:], pub)
	copy(privKey[:], priv)

	pubKeyJSON, err := json.Marshal(pubKey)
	assert.NoError(t, err)

	privKeyJSON, err := json.Marshal(privKey)
	assert.NoError(t, err)

	assert.Equal(t, "\""+hex.EncodeToString(pub)+"\"", string(pubKeyJSON))
	assert.Equal(t, "\""+hex.EncodeToString(priv)+"\"", string(privKeyJSON))
}
