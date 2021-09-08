# go-eque
Exclusive message queue built upon Redis, written in Go

## Installation

go-eque may be installed using the go get command:
```
go get github.com/pghq/go-eque
```
## Usage

```
import "github.com/pghq/go-eque/eque"
```

To create a new queue:

```
queue, err := eque.NewQueue("name-of-your-queue")
if err != nil{
    panic(err)
}
```


p.s. - it's pronounced EQ.