package conf

//注意,配置文件config.conf中的组名[DbConfig]和结构体名称一样.
type DbConfig struct {
	User         string `conf:"user_name"`    //数据库登录用户名
	Pwd          string `conf:"password"`     //数据库密码
	Ip           string `conf:"ip_address"`   //IP地址
	Port         string `conf:"ip_port"`      //端口
	DatabaseName string `conf:"db_name"`      //数据库名
	ConnectInfo  string `conf:"connect_info"` //连接时其它配置.
}

//RPC配置
type RpcServer struct {
	RpcType  string `conf:"rpc_type"`   //rpc类型
	Protocol string `conf:"protocol"`   //rpc协议
	Ip       string `conf:"ip_address"` //rpc的IP地址
	Port     string `conf:"ip_port"`    //rpc的端口
}

//GRPC配置
type GrpcServer struct {
	Protocol string `conf:"protocol"`   //Grpc协议
	Ip       string `conf:"ip_address"` //Grpc的IP地址
	Port     string `conf:"ip_port"`    //Grpc的端口
}
