# go-red

Golang distributed job scheduling.

## Installation

go-red may be installed using the go get command:
```
go get github.com/pghq/go-red
```
## Usage

```
import "github.com/pghq/go-red"
```

To create a new queue:

```
queue := red.New("redis://user:pass@example.com?queue=messages")
if err := queue.Enqueue(context.TODO(), "key", "value"); err != nil{
    panic(err)
}
```

## Powered By
- Redis - https://redis.io