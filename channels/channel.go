package channels

import (
	"MSSQLParser/db"
	"context"
)

type BroadcastServer struct {
	source         <-chan db.Table
	listeners      []chan db.Table
	addListener    chan chan db.Table
	removeListener chan (<-chan db.Table)
}

func (s *BroadcastServer) Subscribe() <-chan db.Table {
	newListener := make(chan db.Table)
	s.addListener <- newListener
	return newListener
}

func (s *BroadcastServer) CancelSubscription(channel <-chan db.Table) {
	s.removeListener <- channel
}

func (s *BroadcastServer) Serve(ctx context.Context) {
	defer func() {
		for _, listener := range s.listeners {
			if listener != nil {
				close(listener)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case newListener := <-s.addListener:
			s.listeners = append(s.listeners, newListener)
		case listenerToRemove := <-s.removeListener:
			for i, ch := range s.listeners {
				if ch == listenerToRemove {
					s.listeners[i] = s.listeners[len(s.listeners)-1]
					s.listeners = s.listeners[:len(s.listeners)-1]
					close(ch)
					break
				}
			}
		case val, ok := <-s.source:

			if !ok {
				return
			}

			for _, listener := range s.listeners {

				if listener != nil {
					select {
					case listener <- val:
					case <-ctx.Done():
						return
					}

				}
			}
		}
	}
}

func NewBroadcastServer(ctx context.Context, source <-chan db.Table) BroadcastServer {
	broadcastService := BroadcastServer{
		source:         source,
		listeners:      make([]chan db.Table, 0),
		addListener:    make(chan chan db.Table),
		removeListener: make(chan (<-chan db.Table)),
	}
	go broadcastService.Serve(ctx)
	return broadcastService
}
