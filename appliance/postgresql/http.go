package main

import (
	"net/http"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/julienschmidt/httprouter"
	"github.com/flynn/flynn/Godeps/_workspace/src/gopkg.in/inconshreveable/log15.v2"
	"github.com/flynn/flynn/appliance/postgresql/client"
	"github.com/flynn/flynn/appliance/postgresql/state"
	"github.com/flynn/flynn/pkg/httphelper"
)

func ServeHTTP(pg *Postgres, peer *state.Peer, log log15.Logger) error {
	api := &HTTP{
		pg:   pg,
		peer: peer,
		log:  log,
	}
	r := httprouter.New()
	r.GET("/status", api.GetStatus)
	return http.ListenAndServe(":5433", r)
}

type HTTP struct {
	pg   *Postgres
	peer *state.Peer
	log  log15.Logger
}

func (h *HTTP) GetStatus(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	res := &pgmanager.Status{
		Peer: h.peer.Info(),
	}
	var err error
	res.Postgres, err = h.pg.Info()
	if err != nil {
		// Log the error, but don't return a 500. We will always have some
		// information to return, but postgres may not be online.
		h.log.Error("error getting postgres info", "err", err)
	}
	httphelper.JSON(w, 200, res)
}
