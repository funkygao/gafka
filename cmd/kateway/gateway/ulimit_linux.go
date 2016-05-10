// +build linux

package gateway

func EnsureValidUlimit() {
	checkUlimit(65535)
}
