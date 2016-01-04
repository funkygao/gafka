// +build linux

package main

func ensureValidUlimit() {
	checkUlimit(65535)
}
