package http

import "strings"

// normalizedEvent menyamakan kapitalisasi eventId dengan usecase (huruf kecil),
// agar langganan SSE di hub cocok dengan channel notifikasi yang dipublikasikan.
func normalizedEvent(eventID string) string { return strings.ToLower(eventID) }
