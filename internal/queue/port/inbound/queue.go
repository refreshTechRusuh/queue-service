// Package inbound berisi driving port: kontrak yang DITAWARKAN domain antrian
// ke pemanggil (HTTP handler, Kafka consumer, sweeper).
package inbound

import (
	"context"

	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/dto"
)

// IQueueUsecase = kontrak antrian.
type IQueueUsecase interface {
	// HTTP
	// Join tanpa identitas apa pun (tanpa login, tanpa NIK) — sesuai flow bisnis.
	Join(ctx context.Context, eventID string) (dto.QueueStatusResponse, error)
	Status(ctx context.Context, eventID, queueToken string) (dto.QueueStatusResponse, error)

	// Event dari BE-2 (Kafka)
	OnStockChanged(ctx context.Context, eventID, categoryID string, available int) error
	OnSlotReleased(ctx context.Context, eventID, queueToken string) error
	OnSoldOut(ctx context.Context, eventID string) error

	// Latar belakang: lepas kursi yang Access Token-nya kedaluwarsa.
	Sweep(ctx context.Context) error
}
