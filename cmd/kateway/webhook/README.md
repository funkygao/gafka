# webhook

webhook worker is a daemon that Sub all webhooked topics messages and 
callback their registered hook endpoints.

Webhooks allow you to build or set up integrations which subscribe to certain events on kateway.

When one of those events is triggered, we'll send a HTTP POST payload to the webhook's configured URL. 

Used with [djob](http://github.com/funkygao/djob) for distributed cluster management.

### Payload

#### Delivery Headers

|| Header || Description ||
|| X-Partition || Partition id of this message ||
|| X-Offset || Offset of this message ||
|| User-Agent || webhooker/{version} ||
