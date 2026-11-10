package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unit test tanpa Postgres. (stock_source_test.go = integration, di-skip bila DB tak ada.)
// Fake dibuat sendiri karena pgxmock v3.4.0 belum cocok dengan pgx v5.11.

type stubQuerier struct {
	rows    [][]any
	rowsErr error
	err     error
	gotSQL  string
	gotArgs []any
}

func (q *stubQuerier) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.gotSQL, q.gotArgs = sql, args
	if q.err != nil {
		return nil, q.err
	}
	return &stubRows{rows: q.rows, err: q.rowsErr, idx: -1}, nil
}

type stubRows struct {
	rows [][]any
	err  error
	idx  int
}

func (r *stubRows) Close()                                       {}
func (r *stubRows) Err() error                                   { return r.err }
func (r *stubRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *stubRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *stubRows) Values() ([]any, error)                       { return r.rows[r.idx], nil }
func (r *stubRows) RawValues() [][]byte                          { return nil }
func (r *stubRows) Conn() *pgx.Conn                              { return nil }
func (r *stubRows) TypeMap() *pgtype.Map                         { return nil }
func (r *stubRows) Next() bool                                   { r.idx++; return r.idx < len(r.rows) }
func (r *stubRows) Scan(dest ...any) error {
	for i, d := range dest {
		dv, sv := reflect.ValueOf(d).Elem(), reflect.ValueOf(r.rows[r.idx][i])
		if !sv.Type().AssignableTo(dv.Type()) {
			return fmt.Errorf("scan kolom %d: %s tidak bisa ke %s", i, sv.Type(), dv.Type())
		}
		dv.Set(sv)
	}
	return nil
}

func TestUnitLoadStock_ShouldReturnAvailablePerCategory(t *testing.T) {
	q := &stubQuerier{rows: [][]any{{"cat-1", 100}, {"cat-2", 0}}}

	got, err := NewStockSource(q).LoadStock(context.Background(), "evt-1")

	require.NoError(t, err)
	assert.Equal(t, map[string]int{"cat-1": 100, "cat-2": 0}, got)
	assert.Equal(t, []any{"evt-1"}, q.gotArgs, "eventId harus lewat parameter, bukan string SQL")
	assert.Contains(t, q.gotSQL, "FROM ticket_category")
	assert.NotContains(t, q.gotSQL, "evt-1")
}

func TestUnitLoadStock_ShouldReturnEmptyMap_WhenEventHasNoCategories(t *testing.T) {
	got, err := NewStockSource(&stubQuerier{}).LoadStock(context.Background(), "evt-1")

	require.NoError(t, err)
	assert.Empty(t, got)
	assert.NotNil(t, got)
}

func TestUnitLoadStock_ShouldReturnError_WhenQueryFails(t *testing.T) {
	boom := errors.New("db down")

	_, err := NewStockSource(&stubQuerier{err: boom}).LoadStock(context.Background(), "evt-1")

	assert.ErrorIs(t, err, boom)
}

func TestUnitLoadStock_ShouldReturnError_WhenScanFails(t *testing.T) {
	q := &stubQuerier{rows: [][]any{{"cat-1", "bukan-angka"}}}

	_, err := NewStockSource(q).LoadStock(context.Background(), "evt-1")

	assert.Error(t, err)
}

func TestUnitLoadStock_ShouldReturnError_WhenIterationFails(t *testing.T) {
	boom := errors.New("koneksi putus")

	_, err := NewStockSource(&stubQuerier{rowsErr: boom}).LoadStock(context.Background(), "evt-1")

	assert.ErrorIs(t, err, boom)
}
