// +build linux

package gateway

func EnsureServerUlimit() {
	checkUlimit(65535)
}
