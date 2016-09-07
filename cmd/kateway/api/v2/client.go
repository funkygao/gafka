package pubsub

type Client struct {
}

func New(options ...func(c *Client) error) (*Client, error) {
	c := &Client{}
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}
