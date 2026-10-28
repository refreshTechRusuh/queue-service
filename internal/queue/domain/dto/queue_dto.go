// Package dto berisi bentuk request/response antrian (dengan tag JSON).
package dto

// JoinRequest = body opsional saat masuk antrian.
// NIK boleh kosong: guest biasanya belum mengisi NIK saat antre (diisi di Detail Ticket).
// Bila diisi, aturan 1 NIK = 1 antrian aktif per event langsung ditegakkan.
type JoinRequest struct {
	NIK string `json:"nik,omitempty"`
}

// QueueStatusResponse = isi field "data" untuk join & cek posisi.
type QueueStatusResponse struct {
	Status           string `json:"status"` // QUEUED | ADMITTED | EXPIRED | SOLD_OUT
	QueueToken       string `json:"queueToken"`
	Position         int64  `json:"position,omitempty"`   // hanya saat QUEUED (1 = paling depan)
	EtaSeconds       *int64 `json:"etaSeconds,omitempty"` // estimasi; disembunyikan bila laju belum terukur
	AccessToken      string `json:"accessToken,omitempty"`
	BookingExpiresAt string `json:"bookingExpiresAt,omitempty"` // RFC3339 UTC
}

// ---- Payload event SSE ----

// SSEPosition = event `position`.
type SSEPosition struct {
	Position   int64  `json:"position"`
	EtaSeconds *int64 `json:"etaSeconds,omitempty"`
}

// SSEAdmitted = event `admitted` (klien redirect ke Detail Ticket).
type SSEAdmitted struct {
	AccessToken      string `json:"accessToken"`
	BookingExpiresAt string `json:"bookingExpiresAt"`
}

// SSEMessage = event `sold_out` / `expired`.
type SSEMessage struct {
	Message string `json:"message"`
}
