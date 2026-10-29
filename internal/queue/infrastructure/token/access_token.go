// Package token (infrastructure) menerbitkan Access Token (JWT HS256).
//
// Bentuk claim HARUS cocok dengan AccessTokenClaims di booking/event/payment-service:
//
//	{ eventId, queueToken, bookingTtlExpiresAt (unix), exp, iat, sub }
//
// (booking-service punya claim `nik` opsional; queue tidak mengisinya karena join
// antrian memang tanpa identitas.)
//
// dan ditandatangani dengan ACCESS_TOKEN_SECRET yang SAMA dengan service tersebut.
// Token self-verifiable: service lain memvalidasi lokal tanpa memanggil queue.
package token

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"

	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/entity"
)

type accessClaims struct {
	EventID             string `json:"eventId"`
	QueueToken          string `json:"queueToken"`
	BookingTTLExpiresAt int64  `json:"bookingTtlExpiresAt"`
	jwt.RegisteredClaims
}

// Signer = implementasi outbound.IAccessTokenSigner.
type Signer struct {
	secret []byte
}

func NewSigner(secret string) *Signer { return &Signer{secret: []byte(secret)} }

// Sign deterministik: claim yang sama (termasuk iat/exp) → string token yang sama.
// Karena itu token tak perlu disimpan; cukup dibangun ulang dari state di Redis.
func (s *Signer) Sign(c entity.AccessClaims) (string, error) {
	claims := accessClaims{
		EventID:             c.EventID,
		QueueToken:          c.QueueToken,
		BookingTTLExpiresAt: c.ExpiresAt.Unix(),
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "queue-service",
			Subject:   c.QueueToken,
			IssuedAt:  jwt.NewNumericDate(c.IssuedAt),
			ExpiresAt: jwt.NewNumericDate(c.ExpiresAt),
		},
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
	if err != nil {
		return "", fmt.Errorf("sign access token: %w", err)
	}
	return signed, nil
}
