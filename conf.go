package kodo

type ZoneConf struct {
	SrcUpHosts []string `yaml:"srcUpHosts"`
	CdnUpHosts []string `yaml:"cdnUpHosts"`
	RsHost     string   `yaml:"rsHost"`
	RsfHost    string   `yaml:"rsfHost"`
	ApiHost    string   `yaml:"apiHost"`
	IoVipHost  string   `yaml:"ioVipHost"`
}
