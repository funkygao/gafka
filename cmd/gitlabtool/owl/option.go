package main

var (
	options struct {
		topic          string
		logfile        string
		webhookOnly    bool
		nonWebhookOnly bool
		debug          bool
		mock           bool
	}
)
