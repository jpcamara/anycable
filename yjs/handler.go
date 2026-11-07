package yjs

import (
	"log/slog"
	"net/http"

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
func WebSocketHandler(n *node.Node, c *config.Config, l *slog.Logger) (http.Handler, error) {
	extractor := server.DefaultHeadersExtractor{Headers: c.RPC.ProxyHeaders, Cookies: c.RPC.ProxyCookies}
	if c.JWT.Enabled() {
		extractor.AuthHeader = c.JWT.HeaderKey()
	}

	return ws.WebsocketHandler(nil, &extractor, &c.WS, l, func(wsc *websocket.Conn, info *server.RequestInfo, callback func()) error {
		session := node.NewSession(n, ws.NewConnection(wsc), info.URL, info.Headers, info.UID,
			node.WithEncoder(NewEncoder()),
		)

		if _, err := n.Authenticate(session); err != nil {
			return err
		}

		return session.Serve(callback)
	}), nil
}
