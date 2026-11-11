// Test cabang usecase memakai STUB store (tanpa Redis) untuk jalur error yang sulit
// dipicu di Redis sungguhan: store gagal, signer gagal, notifikasi gagal, dst.
// Perilaku atomik/konkuren tetap dibuktikan di queue_usecase_test.go (Redis/miniredis).
package usecase_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/refreshTechRusuh/queue-service/internal/queue/application/usecase"
	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/entity"
	"github.com/refreshTechRusuh/queue-service/internal/queue/port/outbound"
	"github.com/refreshTechRusuh/queue-service/pkg/apperror"
)

var errBoom = errors.New("boom")

// ---- stub store: tiap method bisa diganti per test; default = sukses "kosong" ----

type stubStore struct {
	hasStock    func(eventID string) (bool, error)
	warmStock   func(eventID string, m map[string]int) error
	setStock    func(eventID, cat string, n int) error
	join        func(p outbound.JoinParams) (outbound.JoinResult, error)
	state       func(eventID, token string) (outbound.TokenState, error)
	promote     func(eventID string) ([]string, error)
	release     func(eventID, token string) (bool, []string, error)
	sweep       func(eventID string) ([]string, error)
	closeQ      func(eventID string) (int, error)
	register    func(eventID string) error
	active      func() ([]string, error)
	rate        func(eventID string) (float64, error)
	releaseCall int
	promoteCall int
	lastEventID string
}

func (s *stubStore) HasStock(_ context.Context, e string) (bool, error) {
	s.lastEventID = e
	if s.hasStock != nil {
		return s.hasStock(e)
	}
	return true, nil
}
func (s *stubStore) WarmStock(_ context.Context, e string, m map[string]int) error {
	if s.warmStock != nil {
		return s.warmStock(e, m)
	}
	return nil
}
func (s *stubStore) SetStock(_ context.Context, e, c string, n int) error {
	if s.setStock != nil {
		return s.setStock(e, c, n)
	}
	return nil
}
func (s *stubStore) Join(_ context.Context, p outbound.JoinParams) (outbound.JoinResult, error) {
	s.lastEventID = p.EventID
	if s.join != nil {
		return s.join(p)
	}
	return outbound.JoinResult{Outcome: entity.OutcomeQueued, Position: 1}, nil
}
func (s *stubStore) State(_ context.Context, e, t string) (outbound.TokenState, error) {
	if s.state != nil {
		return s.state(e, t)
	}
	return outbound.TokenState{Found: true, Status: entity.StatusQueued, Position: 4}, nil
}
func (s *stubStore) Promote(_ context.Context, e string) ([]string, error) {
	s.promoteCall++
	if s.promote != nil {
		return s.promote(e)
	}
	return nil, nil
}
func (s *stubStore) Release(_ context.Context, e, t string) (bool, []string, error) {
	s.releaseCall++
	if s.release != nil {
		return s.release(e, t)
	}
	return true, nil, nil
}
func (s *stubStore) SweepExpired(_ context.Context, e string) ([]string, error) {
	if s.sweep != nil {
		return s.sweep(e)
	}
	return nil, nil
}
func (s *stubStore) Close(_ context.Context, e string) (int, error) {
	if s.closeQ != nil {
		return s.closeQ(e)
	}
	return 0, nil
}
func (s *stubStore) RegisterEvent(_ context.Context, e string) error {
	if s.register != nil {
		return s.register(e)
	}
	return nil
}
func (s *stubStore) ActiveEvents(context.Context) ([]string, error) {
	if s.active != nil {
		return s.active()
	}
	return nil, nil
}
func (s *stubStore) AdmissionRatePerSec(_ context.Context, e string) (float64, error) {
	if s.rate != nil {
		return s.rate(e)
	}
	return 0, nil
}

type stubSource struct {
	load  func(eventID string) (map[string]int, error)
	calls int
}

func (s *stubSource) LoadStock(_ context.Context, e string) (map[string]int, error) {
	s.calls++
	if s.load != nil {
		return s.load(e)
	}
	return map[string]int{"cat-1": 10}, nil
}

type stubNotify struct {
	err      error
	tokens   []string
	allCalls int
}

func (n *stubNotify) Notify(_ context.Context, _ string, tokens []string) error {
	n.tokens = append(n.tokens, tokens...)
	return n.err
}
func (n *stubNotify) NotifyAll(context.Context, string) error { n.allCalls++; return n.err }

type stubSigner struct{ err error }

func (s stubSigner) Sign(c entity.AccessClaims) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	return "jwt-for-" + c.QueueToken, nil
}

type rig struct {
	uc     *usecase.QueueUsecase
	store  *stubStore
	source *stubSource
	notify *stubNotify
}

func newRig(signerErr error) *rig {
	r := &rig{store: &stubStore{}, source: &stubSource{}, notify: &stubNotify{}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r.uc = usecase.NewQueueUsecase(r.store, r.source, r.notify, stubSigner{err: signerErr},
		usecase.Settings{BookingTTL: 5 * time.Minute}, log)
	return r
}

func newToken() string { return "q_" + uuid.NewString() }

// =================================================================== Join

func TestJoin_ShouldRejectInvalidEventID(t *testing.T) {
	r := newRig(nil)

	for _, id := range []string{"", "abc", "{" + uuid.NewString() + "}", "urn:uuid:" + uuid.NewString()} {
		_, err := r.uc.Join(context.Background(), id)
		assert.Equal(t, apperror.ErrValidation, errCode(err), "id=%q", id)
	}
}

func TestJoin_ShouldNormaliseEventIDToLowercase(t *testing.T) {
	r := newRig(nil)
	id := uuid.NewString()

	_, err := r.uc.Join(context.Background(), strings.ToUpper(id))

	require.NoError(t, err)
	assert.Equal(t, id, r.store.lastEventID, "kapitalisasi berbeda tak boleh memecah antrian")
}

func TestJoin_ShouldReturnInternal_WhenStoreStepFails(t *testing.T) {
	cases := map[string]func(s *stubStore){
		"HasStock":      func(s *stubStore) { s.hasStock = func(string) (bool, error) { return false, errBoom } },
		"RegisterEvent": func(s *stubStore) { s.register = func(string) error { return errBoom } },
		"Join": func(s *stubStore) {
			s.join = func(outbound.JoinParams) (outbound.JoinResult, error) { return outbound.JoinResult{}, errBoom }
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := newRig(nil)
			mutate(r.store)

			_, err := r.uc.Join(context.Background(), uuid.NewString())

			assert.Equal(t, apperror.ErrInternal, errCode(err))
		})
	}
}

func TestJoin_ShouldMapOutcomesToErrors(t *testing.T) {
	cases := map[entity.JoinOutcome]apperror.ErrorCode{
		entity.OutcomeSoldOut: apperror.ErrSoldOut,
		entity.OutcomeFull:    apperror.ErrQueueFull,
	}
	for outcome, want := range cases {
		t.Run(string(outcome), func(t *testing.T) {
			r := newRig(nil)
			r.store.join = func(outbound.JoinParams) (outbound.JoinResult, error) {
				return outbound.JoinResult{Outcome: outcome}, nil
			}

			_, err := r.uc.Join(context.Background(), uuid.NewString())

			assert.Equal(t, want, errCode(err))
		})
	}
}

func TestJoin_ShouldIssueAccessToken_WhenAdmitted(t *testing.T) {
	r := newRig(nil)
	exp := time.Now().Add(5 * time.Minute)
	r.store.join = func(p outbound.JoinParams) (outbound.JoinResult, error) {
		return outbound.JoinResult{Outcome: entity.OutcomeAdmitted, ExpiresAt: exp}, nil
	}

	res, err := r.uc.Join(context.Background(), uuid.NewString())

	require.NoError(t, err)
	assert.Equal(t, "ADMITTED", res.Status)
	assert.Equal(t, "jwt-for-"+res.QueueToken, res.AccessToken)
	assert.True(t, strings.HasPrefix(res.QueueToken, "q_"))
	assert.Equal(t, exp.UTC().Format(time.RFC3339), res.BookingExpiresAt)
}

func TestJoin_ShouldReturnInternal_WhenSignerFails(t *testing.T) {
	r := newRig(errBoom)
	r.store.join = func(outbound.JoinParams) (outbound.JoinResult, error) {
		return outbound.JoinResult{Outcome: entity.OutcomeAdmitted, ExpiresAt: time.Now().Add(time.Minute)}, nil
	}

	_, err := r.uc.Join(context.Background(), uuid.NewString())

	assert.Equal(t, apperror.ErrInternal, errCode(err))
}

func TestJoin_ShouldIncludeETA_OnlyWhenAdmissionRateKnown(t *testing.T) {
	t.Run("laju terukur → ETA = ceil(posisi/laju)", func(t *testing.T) {
		r := newRig(nil)
		r.store.join = func(outbound.JoinParams) (outbound.JoinResult, error) {
			return outbound.JoinResult{Outcome: entity.OutcomeQueued, Position: 10}, nil
		}
		r.store.rate = func(string) (float64, error) { return 4, nil }

		res, err := r.uc.Join(context.Background(), uuid.NewString())

		require.NoError(t, err)
		require.NotNil(t, res.EtaSeconds)
		assert.Equal(t, int64(3), *res.EtaSeconds) // ceil(10/4)
		assert.Equal(t, int64(10), res.Position)
	})
	t.Run("laju 0 → tanpa ETA", func(t *testing.T) {
		r := newRig(nil)
		r.store.rate = func(string) (float64, error) { return 0, nil }
		res, err := r.uc.Join(context.Background(), uuid.NewString())
		require.NoError(t, err)
		assert.Nil(t, res.EtaSeconds)
	})
	t.Run("error baca laju → tanpa ETA (bukan gagal)", func(t *testing.T) {
		r := newRig(nil)
		r.store.rate = func(string) (float64, error) { return 0, errBoom }
		res, err := r.uc.Join(context.Background(), uuid.NewString())
		require.NoError(t, err)
		assert.Nil(t, res.EtaSeconds)
	})
}

func TestJoin_ShouldNotifyPromotedTokens_AndSurviveNotifierFailure(t *testing.T) {
	r := newRig(nil)
	r.notify.err = errBoom
	r.store.join = func(outbound.JoinParams) (outbound.JoinResult, error) {
		return outbound.JoinResult{Outcome: entity.OutcomeQueued, Position: 2, Promoted: []string{"q_a", "q_b"}}, nil
	}

	_, err := r.uc.Join(context.Background(), uuid.NewString())

	require.NoError(t, err, "gagal notifikasi tidak boleh menggagalkan join")
	assert.Equal(t, []string{"q_a", "q_b"}, r.notify.tokens)
}

// ---- warm-up stok (ensureStock/loadStock) lewat Join ----

func TestJoin_ShouldSkipLoadingStock_WhenCacheAlreadyWarm(t *testing.T) {
	r := newRig(nil)

	_, err := r.uc.Join(context.Background(), uuid.NewString())

	require.NoError(t, err)
	assert.Zero(t, r.source.calls)
}

func TestJoin_ShouldWarmUpStockFromSource_WhenCacheEmpty(t *testing.T) {
	r := newRig(nil)
	var warmed map[string]int
	r.store.hasStock = func(string) (bool, error) { return false, nil }
	r.store.warmStock = func(_ string, m map[string]int) error { warmed = m; return nil }

	_, err := r.uc.Join(context.Background(), uuid.NewString())

	require.NoError(t, err)
	assert.Equal(t, map[string]int{"cat-1": 10}, warmed)
	assert.Equal(t, 1, r.source.calls)
}

func TestJoin_ShouldSkipSource_WhenFilledByAnotherInstanceMeanwhile(t *testing.T) {
	r := newRig(nil)
	calls := 0
	r.store.hasStock = func(string) (bool, error) { calls++; return calls > 1, nil } // kosong lalu terisi

	_, err := r.uc.Join(context.Background(), uuid.NewString())

	require.NoError(t, err)
	assert.Zero(t, r.source.calls, "tidak perlu membaca DB bila cache sudah diisi di sela-sela")
}

func TestJoin_ShouldFailWarmUp(t *testing.T) {
	cases := map[string]struct {
		prep func(r *rig)
		want apperror.ErrorCode
	}{
		"DB gagal → INTERNAL": {func(r *rig) {
			r.source.load = func(string) (map[string]int, error) { return nil, errBoom }
		}, apperror.ErrInternal},
		"event tanpa kategori → NOT_FOUND": {func(r *rig) {
			r.source.load = func(string) (map[string]int, error) { return map[string]int{}, nil }
		}, apperror.ErrNotFound},
		"isi cache gagal → INTERNAL": {func(r *rig) {
			r.store.warmStock = func(string, map[string]int) error { return errBoom }
		}, apperror.ErrInternal},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := newRig(nil)
			r.store.hasStock = func(string) (bool, error) { return false, nil }
			tc.prep(r)

			_, err := r.uc.Join(context.Background(), uuid.NewString())

			assert.Equal(t, tc.want, errCode(err))
		})
	}
}

// ================================================================== Status

func TestStatus_ShouldValidateInput(t *testing.T) {
	r := newRig(nil)

	_, err := r.uc.Status(context.Background(), "bukan-uuid", newToken())
	assert.Equal(t, apperror.ErrValidation, errCode(err))

	for _, tok := range []string{"", "abc", uuid.NewString(), "q_xyz", strings.ToUpper(newToken())} {
		_, err = r.uc.Status(context.Background(), uuid.NewString(), tok)
		assert.Equal(t, apperror.ErrValidation, errCode(err), "token=%q", tok)
	}
}

func TestStatus_ShouldMapStoreStateToResponse(t *testing.T) {
	future := time.Now().Add(time.Minute)
	past := time.Now().Add(-time.Minute)
	cases := []struct {
		name       string
		state      outbound.TokenState
		stateErr   error
		wantStatus string
		wantErr    apperror.ErrorCode
		wantAccess bool
	}{
		{"store error → INTERNAL", outbound.TokenState{}, errBoom, "", apperror.ErrInternal, false},
		{"tak dikenal → NOT_FOUND", outbound.TokenState{Found: false}, nil, "", apperror.ErrNotFound, false},
		{"ADMITTED valid → akses", outbound.TokenState{Found: true, Status: entity.StatusAdmitted, ExpiresAt: future}, nil, "ADMITTED", "", true},
		{"ADMITTED lewat TTL → EXPIRED walau sweeper belum jalan", outbound.TokenState{Found: true, Status: entity.StatusAdmitted, ExpiresAt: past}, nil, "EXPIRED", "", false},
		{"QUEUED → posisi", outbound.TokenState{Found: true, Status: entity.StatusQueued, Position: 4}, nil, "QUEUED", "", false},
		{"SOLD_OUT", outbound.TokenState{Found: true, Status: entity.StatusSoldOut}, nil, "SOLD_OUT", "", false},
		{"RELEASED → EXPIRED", outbound.TokenState{Found: true, Status: entity.StatusReleased}, nil, "EXPIRED", "", false},
		{"EXPIRED → EXPIRED", outbound.TokenState{Found: true, Status: entity.StatusExpired}, nil, "EXPIRED", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(nil)
			r.store.state = func(string, string) (outbound.TokenState, error) { return tc.state, tc.stateErr }
			tok := newToken()

			res, err := r.uc.Status(context.Background(), uuid.NewString(), tok)

			if tc.wantErr != "" {
				assert.Equal(t, tc.wantErr, errCode(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, res.Status)
			assert.Equal(t, tok, res.QueueToken)
			assert.Equal(t, tc.wantAccess, res.AccessToken != "")
		})
	}
}

func TestStatus_ShouldReturnInternal_WhenSignerFailsForAdmitted(t *testing.T) {
	r := newRig(errBoom)
	r.store.state = func(string, string) (outbound.TokenState, error) {
		return outbound.TokenState{Found: true, Status: entity.StatusAdmitted, ExpiresAt: time.Now().Add(time.Minute)}, nil
	}

	_, err := r.uc.Status(context.Background(), uuid.NewString(), newToken())

	assert.Equal(t, apperror.ErrInternal, errCode(err))
}

// ============================================================ Event BE-2

func TestOnStockChanged_ShouldIgnoreInvalidPayload(t *testing.T) {
	r := newRig(nil)
	called := false
	r.store.setStock = func(string, string, int) error { called = true; return nil }

	assert.NoError(t, r.uc.OnStockChanged(context.Background(), "bukan-uuid", "cat-1", 5))
	assert.NoError(t, r.uc.OnStockChanged(context.Background(), uuid.NewString(), "", 5))

	assert.False(t, called, "payload rusak tak boleh menyentuh stok (dan tidak di-retry)")
}

func TestOnStockChanged_ShouldSetStockThenPromoteAndNotify(t *testing.T) {
	r := newRig(nil)
	var gotCat string
	var gotN int
	r.store.setStock = func(_ string, c string, n int) error { gotCat, gotN = c, n; return nil }
	r.store.promote = func(string) ([]string, error) { return []string{"q_x"}, nil }

	err := r.uc.OnStockChanged(context.Background(), uuid.NewString(), "cat-1", 7)

	require.NoError(t, err)
	assert.Equal(t, "cat-1", gotCat)
	assert.Equal(t, 7, gotN)
	assert.Equal(t, []string{"q_x"}, r.notify.tokens)
}

func TestOnStockChanged_ShouldContinue_WhenWarmUpFails(t *testing.T) {
	r := newRig(nil)
	r.store.hasStock = func(string) (bool, error) { return false, errBoom }
	setCalled := false
	r.store.setStock = func(string, string, int) error { setCalled = true; return nil }

	err := r.uc.OnStockChanged(context.Background(), uuid.NewString(), "cat-1", 3)

	require.NoError(t, err)
	assert.True(t, setCalled, "warm-up gagal tidak boleh membuang update stok terbaru")
}

func TestOnStockChanged_ShouldReturnError_SoKafkaRetries(t *testing.T) {
	t.Run("SetStock gagal", func(t *testing.T) {
		r := newRig(nil)
		r.store.setStock = func(string, string, int) error { return errBoom }
		assert.ErrorIs(t, r.uc.OnStockChanged(context.Background(), uuid.NewString(), "c", 1), errBoom)
	})
	t.Run("Promote gagal", func(t *testing.T) {
		r := newRig(nil)
		r.store.promote = func(string) ([]string, error) { return nil, errBoom }
		assert.ErrorIs(t, r.uc.OnStockChanged(context.Background(), uuid.NewString(), "c", 1), errBoom)
	})
}

func TestOnSlotReleased(t *testing.T) {
	t.Run("eventId rusak → diabaikan", func(t *testing.T) {
		r := newRig(nil)
		assert.NoError(t, r.uc.OnSlotReleased(context.Background(), "x", newToken()))
		assert.Zero(t, r.store.releaseCall+r.store.promoteCall)
	})
	t.Run("dengan queueToken → Release (bukan Promote)", func(t *testing.T) {
		r := newRig(nil)
		r.store.release = func(string, string) (bool, []string, error) { return true, []string{"q_next"}, nil }

		require.NoError(t, r.uc.OnSlotReleased(context.Background(), uuid.NewString(), newToken()))

		assert.Equal(t, 1, r.store.releaseCall)
		assert.Zero(t, r.store.promoteCall)
		assert.Equal(t, []string{"q_next"}, r.notify.tokens)
	})
	t.Run("tanpa queueToken → hanya Promote", func(t *testing.T) {
		r := newRig(nil)

		require.NoError(t, r.uc.OnSlotReleased(context.Background(), uuid.NewString(), ""))

		assert.Zero(t, r.store.releaseCall)
		assert.Equal(t, 1, r.store.promoteCall)
	})
	t.Run("queueToken berformat salah → diperlakukan tanpa token", func(t *testing.T) {
		r := newRig(nil)
		require.NoError(t, r.uc.OnSlotReleased(context.Background(), uuid.NewString(), "sampah"))
		assert.Zero(t, r.store.releaseCall)
		assert.Equal(t, 1, r.store.promoteCall)
	})
	t.Run("Release gagal → error (di-retry)", func(t *testing.T) {
		r := newRig(nil)
		r.store.release = func(string, string) (bool, []string, error) { return false, nil, errBoom }
		assert.ErrorIs(t, r.uc.OnSlotReleased(context.Background(), uuid.NewString(), newToken()), errBoom)
	})
	t.Run("Promote gagal → error (di-retry)", func(t *testing.T) {
		r := newRig(nil)
		r.store.promote = func(string) ([]string, error) { return nil, errBoom }
		assert.ErrorIs(t, r.uc.OnSlotReleased(context.Background(), uuid.NewString(), ""), errBoom)
	})
}

func TestOnSoldOut(t *testing.T) {
	t.Run("eventId rusak → diabaikan", func(t *testing.T) {
		r := newRig(nil)
		assert.NoError(t, r.uc.OnSoldOut(context.Background(), "x"))
		assert.Zero(t, r.notify.allCalls)
	})
	t.Run("sukses → NotifyAll", func(t *testing.T) {
		r := newRig(nil)
		r.store.closeQ = func(string) (int, error) { return 12, nil }
		require.NoError(t, r.uc.OnSoldOut(context.Background(), uuid.NewString()))
		assert.Equal(t, 1, r.notify.allCalls)
	})
	t.Run("Close gagal → error (di-retry), tanpa notifikasi", func(t *testing.T) {
		r := newRig(nil)
		r.store.closeQ = func(string) (int, error) { return 0, errBoom }
		assert.ErrorIs(t, r.uc.OnSoldOut(context.Background(), uuid.NewString()), errBoom)
		assert.Zero(t, r.notify.allCalls)
	})
	t.Run("notifikasi gagal → tetap sukses (antrian sudah tertutup)", func(t *testing.T) {
		r := newRig(nil)
		r.notify.err = errBoom
		assert.NoError(t, r.uc.OnSoldOut(context.Background(), uuid.NewString()))
	})
}

// =================================================================== Sweep

func TestSweep(t *testing.T) {
	t.Run("ActiveEvents gagal → error", func(t *testing.T) {
		r := newRig(nil)
		r.store.active = func() ([]string, error) { return nil, errBoom }
		assert.ErrorIs(t, r.uc.Sweep(context.Background()), errBoom)
	})
	t.Run("tanpa event aktif → no-op", func(t *testing.T) {
		assert.NoError(t, newRig(nil).uc.Sweep(context.Background()))
	})
	t.Run("satu event gagal tidak menghentikan event lain", func(t *testing.T) {
		r := newRig(nil)
		r.store.active = func() ([]string, error) { return []string{"e-gagal", "e-ok"}, nil }
		var swept []string
		r.store.sweep = func(e string) ([]string, error) {
			swept = append(swept, e)
			if e == "e-gagal" {
				return nil, errBoom
			}
			return []string{"q_promoted"}, nil
		}

		require.NoError(t, r.uc.Sweep(context.Background()))

		assert.Equal(t, []string{"e-gagal", "e-ok"}, swept)
		assert.Equal(t, []string{"q_promoted"}, r.notify.tokens, "yang dipromosikan tetap dikabari")
	})
}
