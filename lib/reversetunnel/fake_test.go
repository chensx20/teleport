/*
Copyright 2021 Gravitational, Inc.

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

package reversetunnel

import (
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFakeRemoteSiteDialError(t *testing.T) {
	connCh := make(chan net.Conn, 1)
	site := &FakeRemoteSite{ConnCh: connCh}
	dialErr := errors.New("host is offline")
	site.SetDialError("offline.cluster", dialErr)

	conn, err := site.Dial(DialParams{ServerID: "offline.cluster"})
	require.ErrorIs(t, err, dialErr)
	require.Nil(t, conn)
	require.Empty(t, connCh)

	conn, err = site.Dial(DialParams{ServerID: "healthy.cluster"})
	require.NoError(t, err)
	defer conn.Close()
	peer := <-connCh
	require.NoError(t, peer.Close())
}
