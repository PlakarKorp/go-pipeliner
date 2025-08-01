package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/PlakarKorp/go-pipeliner"
)

func main() {
	ctx := context.Background()

	pipe := pipeliner.New[int]()

	pipe1 := pipeliner.AddStage(pipe, 2, func(ctx context.Context, i int) (string, error) {
		return strconv.Itoa(i), nil
	})

	pipe2 := pipeliner.AddStage(pipe1, 2, func(ctx context.Context, s string) ([]byte, error) {
		return []byte("[" + s + "]"), nil
	})

	pipe3 := pipeliner.AddStage(pipe2, 2, func(ctx context.Context, foo []byte) ([]byte, error) {
		return foo, nil
	})

	in := make(chan int)
	go func() {
		defer close(in)
		for i := 1; i <= 10; i++ {
			in <- i
		}
	}()

	out, errs := pipe3.Run(ctx, in)

	// Collect output
	for o := range out {
		fmt.Println("Output:", string(o))
	}

	// Handle errors
	for err := range errs {
		fmt.Println("Error:", err)
	}
}
