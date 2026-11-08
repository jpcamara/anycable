package yjs

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/anycable/anycable-go/config"
	"github.com/anycable/anycable-go/node"
	"github.com/anycable/anycable-go/server"
	"github.com/anycable/anycable-go/ws"
	"github.com/gorilla/websocket"
)

// WebSocketHandler serves y-protocols clients. Its signature matches
// cli.WithWebSocketEndpoint, so a runner can mount it at a path like "/yjs/".
// The connection is authenticated with the Rails connection's connect, the
// same as at /cable; the document subscription waits for the auth frame.
//
// ANYCABLE_YJS_WHISPER=false sends awareness through the channel instead of
// whispering it, for apps that validate presence on the server.
func WebSocketHandler(n *node.Node, c *config.Config, l *slog.Logger) (http.Handler, error) {
	whisper := os.Getenv("ANYCABLE_YJS_WHISPER") != "false"

	extractor := server.DefaultHeadersExtractor{Headers: c.RPC.ProxyHeaders, Cookies: c.RPC.ProxyCookies}
	if c.JWT.Enabled() {
		extractor.AuthHeader = c.JWT.HeaderKey()
	}

	return ws.WebsocketHandler(nil, &extractor, &c.WS, l, func(wsc *websocket.Conn, info *server.RequestInfo, callback func()) error {
		encoder := NewEncoder()
		encoder.Whisper = whisper

		session := node.NewSession(n, ws.NewConnection(wsc), info.URL, info.Headers, info.UID,
			node.WithEncoder(encoder),
		)

		if _, err := n.Authenticate(session); err != nil {
			return err
		}

		return session.Serve(callback)
	}), nil
}
