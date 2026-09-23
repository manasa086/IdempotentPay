package payment

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
)

// Handler exposes the payment API over HTTP.
type Handler struct {
	ledger Ledger
}

// NewHandler returns a Handler backed by ledger.
func NewHandler(ledger Ledger) *Handler {
	return &Handler{ledger: ledger}
}

// Routes registers the payment endpoints on mux. The caller is expected to wrap
// mux with an idempotency middleware.
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/charges", h.CreateCharge)
	mux.HandleFunc("GET /v1/charges/{id}", h.GetCharge)
}

type createChargeRequest struct {
	Account     string `json:"account"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

func (req createChargeRequest) valid() bool {
	return req.Account != "" && req.AmountCents > 0 && len(req.Currency) == 3
}

// CreateCharge records a new charge. This is the side-effecting endpoint the
// idempotency layer protects.
func (h *Handler) CreateCharge(w http.ResponseWriter, r *http.Request) {
	var req createChargeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}
	if !req.valid() {
		writeError(w, http.StatusUnprocessableEntity, "account, positive amount_cents, and 3-letter currency are required")
		return
	}

	c, err := h.ledger.CreateCharge(r.Context(), Charge{
		Account:     req.Account,
		AmountCents: req.AmountCents,
		Currency:    req.Currency,
	})
	if err != nil {
		log.Printf("create charge: %v", err)
		writeError(w, http.StatusInternalServerError, "could not record charge")
		return
	}

	writeJSON(w, http.StatusCreated, c)
}

// GetCharge returns a previously recorded charge. It is a safe method and is not
// guarded by the idempotency layer.
func (h *Handler) GetCharge(w http.ResponseWriter, r *http.Request) {
	c, err := h.ledger.GetCharge(r.Context(), r.PathValue("id"))
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "no such charge")
		return
	} else if err != nil {
		log.Printf("get charge: %v", err)
		writeError(w, http.StatusInternalServerError, "could not load charge")
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
