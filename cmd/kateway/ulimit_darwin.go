// +build darwin

package main

func ensureValidUlimit() {
	checkUlimit(10000)
}
