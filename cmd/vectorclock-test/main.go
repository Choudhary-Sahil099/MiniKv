package main

import (
	"fmt"

	"minikv/internal/vectorclock"
)

func main() {

	// are equal
	a := vectorclock.VectorClock{
		"NodeA": 1,
		"NodeB": 2,
	}

	b := vectorclock.VectorClock{
		"NodeA": 1,
		"NodeB": 2,
	}
	result := vectorclock.Compare(a, b)

	fmt.Printf("Type: %T\n", result)
	fmt.Printf("String(): %s\n", result.String())
	fmt.Printf("Println(): ")
	fmt.Println(result)

	//befire

	a = vectorclock.VectorClock{
		"NodeA": 1,
	}

	b = vectorclock.VectorClock{
		"NodeA": 2,
	}

	fmt.Println("Before Test:", vectorclock.Compare(a, b))

	// after

	a = vectorclock.VectorClock{
		"NodeA": 5,
	}

	b = vectorclock.VectorClock{
		"NodeA": 2,
	}

	fmt.Println("After Test :", vectorclock.Compare(a, b))

	// concurrent
	a = vectorclock.VectorClock{
		"NodeA": 2,
		"NodeB": 1,
	}

	b = vectorclock.VectorClock{
		"NodeA": 1,
		"NodeB": 2,
	}

	fmt.Println("Concurrent Test:", vectorclock.Compare(a, b))
}
