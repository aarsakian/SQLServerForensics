package db

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestProcessPagesIgnoresIncompleteLastPage(t *testing.T) {
	mdf, err := os.ReadFile(filepath.Join("..", "Dummy.mdf"))
	if err != nil {
		t.Fatal(err)
	}

	// five complete pages (before the boot page) followed by part of the sixth
	database := Database{Fname: filepath.Join(t.TempDir(), "truncated.mdf")}
	if err := os.WriteFile(database.Fname, mdf[:5*PAGELEN+100], 0600); err != nil {
		t.Fatal(err)
	}
	processedPages, err := database.ProcessMDF(nil, 0, math.MaxInt, false)
	if err != nil {
		t.Fatalf("truncated file: %v", err)
	}
	if processedPages == 0 || processedPages > 5 {
		t.Errorf("truncated file: %d pages processed, want 1 to 5", processedPages)
	}
}
