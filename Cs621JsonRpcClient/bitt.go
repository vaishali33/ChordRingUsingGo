package main

import (
        "fmt"
        "math/big"
)

func main() {
    //var i int
    //i = 8
    i := big.NewInt(7)
    j := i.Bit(2)
    fmt.Println(j)

}
