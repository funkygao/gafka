// +build darwin

package gateway

func EnsureValidUlimit() {
	checkUlimit(10000)
}
