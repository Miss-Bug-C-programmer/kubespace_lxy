package conversion

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

const DefaultMaxConversionBodyBytes int64 = 4 << 20

type Handler struct{ MaxBodyBytes int64 }

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	limit := h.MaxBodyBytes
	if limit <= 0 {
		limit = DefaultMaxConversionBodyBytes
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		http.Error(w, fmt.Sprintf("read ConversionReview: %v", err), http.StatusRequestEntityTooLarge)
		return
	}
	var review apiextensionsv1.ConversionReview
	if err := json.Unmarshal(body, &review); err != nil {
		http.Error(w, fmt.Sprintf("decode ConversionReview: %v", err), http.StatusBadRequest)
		return
	}
	response := ConvertReview(&review)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("encode ConversionReview: %v", err), http.StatusInternalServerError)
	}
}
