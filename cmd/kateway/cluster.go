package main

func (this *Gateway) registerInZk() {
	if err := this.metaStore.RegisterKateway(options.zone, options.cluster, this.hostname,
		options.pubHttpAddr, options.pubHttpsAddr,
		options.subHttpAddr, options.subHttpsAddr); err != nil {
		panic(err)
	}

}
