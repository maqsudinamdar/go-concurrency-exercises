//////////////////////////////////////////////////////////////////////
//
// Given is a producer-consumer scenario, where a producer reads in
// tweets from a mockstream and a consumer is processing the
// data. Your task is to change the code so that the producer as well
// as the consumer can run concurrently
//

package main

import (
	"fmt"
	"sync"
	"time"
)

func producer(stream Stream, tweets chan<- Tweet, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		tweet, err := stream.Next()
		if err == ErrEOF {
			close(tweets)
			return
		} else {
			tweets <- *tweet
		}
	}
}

func consumer(tweets <-chan Tweet, wg *sync.WaitGroup) {
	defer wg.Done()
	for tweet := range tweets {
		if tweet.IsTalkingAboutGo() {
			fmt.Println(tweet.Username, "\ttweets about golang")
		} else {
			fmt.Println(tweet.Username, "\tdoes not tweet about golang")
		}
	}
}

func main() {
	start := time.Now()
	stream := GetMockStream()
	tweet := make(chan Tweet, 3)
	var wg sync.WaitGroup

	wg.Add(2)

	// Producer
	go producer(stream, tweet, &wg)

	// Consumer
	go consumer(tweet, &wg)

	wg.Wait()

	fmt.Printf("Process took %s\n", time.Since(start))
}
