package payment

import (
	"encoding/json"
	"errors"
	"net/http"
)

// Handler exposes the payment API over HTTP.
type Handler struct {
	ledger *Ledger
}

// NewHandler returns a Handler backed by ledger.
func NewHandler(ledger *Ledger) *Handler {
	return &Handler{ledger: ledger}
}

// Routes registers the payment endpoints on mux. The caller is expected to wrap
// mux with the idempotency middleware.
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/charges", h.CreateCharge)
	mux.HandleFunc("GET /v1/charges/{id}", h.GetCharge)
}

type createChargeRequest struct {
	Account     string `json:"account"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

// CreateCharge records a new charge. This is the side-effecting endpoint the
// idempotency layer protects.
func (h *Handler) CreateCharge(w http.ResponseWriter, r *http.Request) {
	var req createChargeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}

	c, err := h.ledger.Charge(req.Account, req.AmountCents, req.Currency)
	if errors.Is(err, ErrInvalidCharge) {
		writeError(w, http.StatusUnprocessableEntity, "account, positive amount_cents, and 3-letter currency are required")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, "could not record charge")
		return
	}

	writeJSON(w, http.StatusCreated, c)
}

// GetCharge returns a previously recorded charge. It is a safe method and is not
// guarded by the idempotency layer.
func (h *Handler) GetCharge(w http.ResponseWriter, r *http.Request) {
	c, ok := h.ledger.Get(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "no such charge")
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
