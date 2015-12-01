package main

func (this *Gateway) registerInZk() {
	if err := this.meta.RegisterKateway(options.zone, options.cluster,
		this.hostname+":"+this.id,
		options.pubHttpAddr, options.pubHttpsAddr,
		options.subHttpAddr, options.subHttpsAddr); err != nil {
		panic(err)
	}

}
