package pipeliner_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/PlakarKorp/go-pipeliner"
)

func TestPipeline_BasicFlow(t *testing.T) {
	pipe := pipeliner.New[int]() // *Pipeline[int, int]

	pipe1 := pipeliner.AddStage(pipe, 1, func(ctx context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	})
	pipe2 := pipeliner.AddStage(pipe1, 2, func(ctx context.Context, s string) ([]byte, error) {
		return []byte("[" + s + "]"), nil
	})

	in := make(chan int)
	go func() {
		defer close(in)
		for i := 1; i <= 5; i++ {
			in <- i
		}
	}()

	ctx := context.Background()
	out, errs := pipe2.Run(ctx, in)

	var results [][]byte
	for val := range out {
		results = append(results, val)
	}

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	for err := range errs {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPipeline_ErrorPropagation(t *testing.T) {
	pipe := pipeliner.New[int]()

	pipe = pipeliner.AddStage(pipe, 1, func(ctx context.Context, i int) (int, error) {
		if i%2 == 0 {
			return 0, errors.New("even number error")
		}
		return i, nil
	})

	in := make(chan int)
	go func() {
		defer close(in)
		for i := 1; i <= 4; i++ {
			in <- i
		}
	}()

	ctx := context.Background()
	out, errs := pipe.Run(ctx, in)

	var wg sync.WaitGroup
	wg.Add(2)

	var results []int
	go func() {
		defer wg.Done()
		for val := range out {
			results = append(results, val)
		}
	}()

	var errorCount int
	go func() {
		defer wg.Done()
		for range errs {
			errorCount++
		}
	}()

	wg.Wait()

	if len(results) != 2 {
		t.Errorf("expected 2 successful results, got %d", len(results))
	}
	if errorCount != 2 {
		t.Errorf("expected 2 errors, got %d", errorCount)
	}
}

func TestPipeline_ContextCancellation(t *testing.T) {
	pipe := pipeliner.New[int]()

	pipe = pipeliner.AddStage(pipe, 1, func(ctx context.Context, i int) (int, error) {
		time.Sleep(10 * time.Millisecond)
		return i, nil
	})

	in := make(chan int)
	go func() {
		defer close(in)
		for i := 0; i < 100; i++ {
			in <- i
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	out, errs := pipe.Run(ctx, in)

	var resultCount int
	for range out {
		resultCount++
	}
	for range errs {
		// Drain errors if any
	}

	if resultCount >= 100 {
		t.Errorf("expected fewer results due to cancellation, got %d", resultCount)
	}
}

func BenchmarkPipeline_SingleStage(b *testing.B) {
	pipe := pipeliner.New[int]()
	pipe = pipeliner.AddStage(pipe, 4, func(ctx context.Context, i int) (int, error) {
		return i * 2, nil
	})

	ctx := context.Background()
	in := make(chan int, b.N)
	go func() {
		for i := 0; i < b.N; i++ {
			in <- i
		}
		close(in)
	}()

	out, errs := pipe.Run(ctx, in)
	for range out {
	}
	for range errs {
	}
}

func BenchmarkPipeline_MultiStage(b *testing.B) {
	pipe := pipeliner.New[int]()

	pipe1 := pipeliner.AddStage(pipe, 4, func(ctx context.Context, i int) (int, error) {
		return i + 1, nil
	})
	pipe2 := pipeliner.AddStage(pipe1, 4, func(ctx context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	})
	pipe3 := pipeliner.AddStage(pipe2, 4, func(ctx context.Context, s string) ([]byte, error) {
		return []byte(s), nil
	})

	ctx := context.Background()
	in := make(chan int, b.N)
	go func() {
		for i := 0; i < b.N; i++ {
			in <- i
		}
		close(in)
	}()

	out, errs := pipe3.Run(ctx, in)
	for range out {
	}
	for range errs {
	}
}
