// +build darwin

package gateway

func EnsureServerUlimit() {
	checkUlimit(10000)
}
