package p2p

// type Handshake interface {
//   Handshake() error
// }

type HandshakeFunc func(any) error

func NOPHandshakeFunc(any) error {
	return nil
}
