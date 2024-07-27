package raft

import "fmt"
import "time"

// Debugging
const Debug3A = false
const Debug3B = false

func DPrint3A(server int, msg string) {
	if Debug3A {
		padding := "    "
		for i := 0; i < server; i++ {
			padding += "        "
		}
		msg = fmt.Sprintf("%s%s%v:%s", time.Now().Format("15:04:05.000000"), padding, server, msg)
		fmt.Printf("\033[1;30;%vm%s\033[0m\n", 34 + server % 10, msg)
	}
}

func DPrint3B(server int, msg string) {
	if Debug3B {
		msg = fmt.Sprintf("%s %v:%s", time.Now().Format("15:04:05.000000"), server, msg)
		fmt.Printf("\033[1;30;%vm%s\033[0m\n", 34 + server % 4, msg)
	}
}
