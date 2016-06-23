package disk

type DiskService struct {
	cf *Config
}

func New(cf *Config) *DiskService {
	return &DiskService{
		cf: cf,
	}
}
