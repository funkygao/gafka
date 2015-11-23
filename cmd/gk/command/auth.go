package command

type AuthFunc func(user, pass string) bool

const (
	adminPasswd = "gAfKa"
)

var (
	Authenticator AuthFunc
)

func init() {
	// default auth component, caller can override this
	Authenticator = func(user, pass string) bool {
		if pass == adminPasswd {
			return true
		}

		return false
	}
}
