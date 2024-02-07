package gopool

import "sync"

type GoroutinesPool struct {
	totalRoundsCount  int
	countPerIteration int
	awaitIndicator    *sync.WaitGroup
}

func NewPool(totalRoundsCount int, countPerIteration int) *GoroutinesPool {
	return &GoroutinesPool{
		totalRoundsCount:  totalRoundsCount,
		countPerIteration: min(countPerIteration, totalRoundsCount),
		awaitIndicator:    &sync.WaitGroup{},
	}
}

func (gp *GoroutinesPool) Run(eachIterFunc func(...any), eachRoundFunc func(int, ...any), args ...any) {
	gp.awaitIndicator.Add(1)

	for i := 0; i < gp.totalRoundsCount; {
		wg := sync.WaitGroup{}
		// wg.Add(gp.countPerIteration)
		j := 0
		for ; j < gp.countPerIteration && i < gp.totalRoundsCount; j++ {
			wg.Add(1)
			
			go func(_wg *sync.WaitGroup) {
				eachIterFunc(args...)
				_wg.Done()
			}(&wg)

			i++
		}

		println("in progress...", j, "goroutines to finish OF", i, "total:", gp.totalRoundsCount)
		wg.Wait()

		if eachRoundFunc != nil {
			eachRoundFunc(j, args...)
		}
	}

	gp.awaitIndicator.Done()
}

func (gp *GoroutinesPool) Await() {
	gp.awaitIndicator.Wait()
}
