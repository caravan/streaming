# Caravan Streaming

[![Go Report Card](https://goreportcard.com/badge/github.com/caravan/streaming)](https://goreportcard.com/report/github.com/caravan/streaming) [![Build Status](https://app.travis-ci.com/caravan/streaming.svg?branch=main)](https://app.travis-ci.com/caravan/streaming) [![Test Coverage](https://api.codeclimate.com/v1/badges/765ab190b974830efb4d/test_coverage)](https://codeclimate.com/github/caravan/streaming/test_coverage) [![Maintainability](https://api.codeclimate.com/v1/badges/765ab190b974830efb4d/maintainability)](https://codeclimate.com/github/caravan/streaming/maintainability) [![GitHub](https://img.shields.io/github/license/caravan/streaming)](https://github.com/caravan/streaming/blob/main/LICENSE.md)

Caravan is a set of in-process event streaming tools for [Go](https://golang.org/) applications. Think ["Kafka"](https://kafka.apache.org), but for the internal workings of your software. Caravan Streaming includes basic features for building Event Streams and Tables.

_This is a work in progress. The basics are there, but not yet ready for production use. Use at your own risk_

## Example

Creates a Producer and two Consumers, each of which consumes from the Producer independently.

```go
package main

import (
    "fmt"
    "math/rand"

    "github.com/caravan/essentials"
	"github.com/caravan/essentials/message"
    "github.com/caravan/essentials/topic"
    "github.com/caravan/streaming/stream/build"
)

func main() {
    // Create new topics with permanent retention
    left := essentials.NewTopic()
    right := essentials.NewTopic()
    out := essentials.NewTopic()

    s, _ := build.
        TopicSource(left).
        Filter(func(e message.Event) bool {
            // Filter out numbers greater than or equal to 200
            return e.(int) < 200
        }).
        Join(
            build.
                TopicSource(right).
                Filter(func(e message.Event) bool {
                    // Filter out numbers less than or equal to 100
                    return e.(int) > 100
                }),
            func(l message.Event, r message.Event) bool {
                // Only join if the left is even, and the right is odd
                return l.(int)%2 == 0 && r.(int)%2 == 1
            },
            func(l message.Event, r message.Event) message.Event {
                // Join by multiplying the numbers
                return l.(int) * r.(int)
            },
        ).
        TopicSink(out).
        Stream()
    _ = s.Start()

    go func() {
        // Start sending stuff to the topic
        lp := left.NewProducer()
        rp := right.NewProducer()
        for i := 0; i < 10000; i++ {
            lp.Send() <- rand.Intn(1000)
            rp.Send() <- rand.Intn(1000)
        }
        lp.Close()
        rp.Close()
    }()

    c := out.NewConsumer()
    for i := 0; i < 10; i++ {
        // Display the first ten that come out
        fmt.Println(message.MustReceive(c))
    }
    c.Close()
}
```
