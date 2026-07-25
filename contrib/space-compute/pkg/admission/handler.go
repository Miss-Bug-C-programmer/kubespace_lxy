package admission

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const DefaultMaxAdmissionBodyBytes int64 = 1 << 20

type Handler struct {
	validator *Validator
	maxBytes  int64
}

func NewHandler(validator *Validator, maxBytes int64) (*Handler, error) {
	if validator == nil {
		return nil, fmt.Errorf("validator is required")
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxAdmissionBodyBytes
	}
	return &Handler{validator: validator, maxBytes: maxBytes}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "" && !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		http.Error(w, "application/json required", http.StatusUnsupportedMediaType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBytes)
	decoder := json.NewDecoder(r.Body)
	var review admissionv1.AdmissionReview
	if err := decoder.Decode(&review); err != nil {
		http.Error(w, "invalid AdmissionReview: "+err.Error(), http.StatusBadRequest)
		return
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		http.Error(w, "AdmissionReview contains trailing JSON", http.StatusBadRequest)
		return
	}
	response := &admissionv1.AdmissionResponse{Allowed: false}
	if review.Request != nil {
		response.UID = review.Request.UID
		if err := h.validator.Validate(r.Context(), review.Request); err != nil {
			response.Result = &metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonInvalid, Message: boundedMessage(err.Error(), 1024), Code: http.StatusUnprocessableEntity}
		} else {
			response.Allowed = true
			response.Result = &metav1.Status{Status: metav1.StatusSuccess, Code: http.StatusOK}
		}
	} else {
		response.Result = &metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonInvalid, Message: "AdmissionReview request is required", Code: http.StatusBadRequest}
	}
	review.Response = response
	review.Request = nil
	if err := json.NewEncoder(w).Encode(&review); err != nil {
		http.Error(w, "encode AdmissionReview response", http.StatusInternalServerError)
	}
}

func boundedMessage(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	if limit < 4 {
		return value[:limit]
	}
	return value[:limit-3] + "..."
}
