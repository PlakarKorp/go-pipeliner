package pipeliner

import (
	"context"
	"sync"
)

type StageFunc[In any, Out any] func(context.Context, In) (Out, error)
type Pipeline[In any, Out any] struct {
	runFn func(ctx context.Context, input <-chan In) (<-chan Out, <-chan error)
}

func New[In any]() *Pipeline[In, In] {
	return &Pipeline[In, In]{
		runFn: func(_ context.Context, in <-chan In) (<-chan In, <-chan error) {
			emptyErrs := make(chan error)
			close(emptyErrs)
			return in, emptyErrs
		},
	}
}

func AddStage[In any, Out any, Next any](p *Pipeline[In, Out], workers int, fn StageFunc[Out, Next]) *Pipeline[In, Next] {
	return &Pipeline[In, Next]{
		runFn: func(ctx context.Context, in <-chan In) (<-chan Next, <-chan error) {
			outPrev, errPrev := p.runFn(ctx, in)
			out := make(chan Next)
			errCurr := make(chan error, workers)

			var wg sync.WaitGroup
			for range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-ctx.Done():
							return
						case item, ok := <-outPrev:
							if !ok {
								return
							}
							res, err := fn(ctx, item)
							if err != nil {
								errCurr <- err
								continue
							}
							select {
							case out <- res:
							case <-ctx.Done():
								return
							}
						}
					}
				}()
			}

			go func() {
				wg.Wait()
				close(out)
				close(errCurr)
			}()

			return out, mergeErrors(errPrev, errCurr)
		},
	}
}

func (p *Pipeline[In, Out]) Run(ctx context.Context, in <-chan In) (<-chan Out, <-chan error) {
	return p.runFn(ctx, in)
}

func mergeErrors(chs ...<-chan error) <-chan error {
	out := make(chan error)
	var wg sync.WaitGroup

	for _, ch := range chs {
		if ch == nil {
			continue
		}
		wg.Add(1)
		go func(c <-chan error) {
			defer wg.Done()
			for err := range c {
				out <- err
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
