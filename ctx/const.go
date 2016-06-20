package ctx

const (
	DefaultConfig = `
{
    zones: [
        {
            name: "mirror"
            zk: "10.77.152.48:2181"
        }
        {
            name: "test"
            zk: "10.213.42.140:12181,10.213.42.141:12181,10.213.42.142:12181"
        }
        {
            name: "sit"
            zk: "10.213.33.154:2181,10.213.42.48:2181,10.213.42.49:2181"
        }
        {
            name: "prod"
            zk: "10.209.33.69:2181,10.209.37.19:2181,10.209.37.68:2181"
        }
        {
            name: "z_app_test"
            zk: "10.213.43.69:2181,10.213.43.70:2181,10.213.43.72:2181"
        }
        {
            name: "z_app_sit"
            zk: "10.213.57.247:2181,10.213.34.37:2181,10.213.57.245:2181"
        }
        {
            name: "z_app_prod"
            zk: "10.213.1.225:2181,10.213.10.140:2181,10.213.18.207:2181,10.213.10.145:2181,10.213.18.215:2181"
        }
        {
            // Qconf
            name: "z_qc_test"
            zk: "10.213.43.87:2181,10.213.43.89:2181,10.213.43.90:2181"
        }
        {
            name: "z_qc_sit"
            zk: "10.213.43.86:2181,10.213.43.88:2181,10.213.34.78:2181"
        }
        {
            name: "z_qc_prod"
            zk: "10.213.2.3:2181,10.213.18.238:2181,10.213.18.233:2181,10.213.18.234:2181,10.213.18.235:2181"
        }
    ]

    zk_default_zone: "prod"
    kafka_home: "/opt/kafka_2.10-0.8.2.2"
    loglevel: "info"

    aliases: [
        {
            cmd: "sublag"
            alias: "lags -z prod -c bigtopic -lag 1 -p"
        }
        {
            cmd: "apache"
            alias: "peek -z prod -c logstash -t logstash_apache -p -1 -body"
        }
        {
            cmd: "gw"
            alias: "peek -z prod -c activitycenter_platform -t gateway_log -body -p -1"
        }
        {
            cmd: "java"
            alias: "peek -z prod -c svc_hippo -t flume_kafka_queue_topic -body"
        }
        {
            cmd: "ngerr"
            alias: "peek -z prod -c logstash -t nginx_errlog"
        }
        {
            cmd: "ng"
            alias: "peek -z prod -c logstash -t nginx_access_log -p -1"
        }
        {
            cmd: "kw"
            alias: "peek -z prod -c logs -t pubsub_log"
        }
        {
            cmd: "app"
            alias: "peek -z prod -c logstash -t app_log -p -1"
        }
        {
            cmd: "h5"
            alias: "peek -z prod -c logstash -t logstash_h5_log -p -1 -body"
        }
        {
            cmd: "order"
            alias: "peek -z prod -c trade -t OrderStatusMsg -p -1 -body"
        }
        {
            cmd: "bizalarm"
            alias: "peek -z prod -c svc_hippo -t flume_biz_alarm -p -1 -body"
        }
    ]

    reverse_dns: [
        // prod qconf zk
        "z2181a-qc.wdds.zk.com:10.213.2.3"
        "z2181b-qc.wdds.zk.com:10.213.18.238"
        "z2181c-qc.wdds.zk.com:10.213.18.233"
        "z2181d-qc.wdds.zk.com:10.213.18.234"
        "z2181e-qc.wdds.zk.com:10.213.18.235"
        
        // sit qconf zk
        "z2181a-qc.sit.wdds.zk.com:10.213.43.86"
        "z2181b-qc.sit.wdds.zk.com:10.213.43.88"
        "z2181c-qc.sit.wdds.zk.com:10.213.34.78"
        
        // test qconf zk
        "z2181a-qc.test.wdds.zk.com:10.213.43.87"
        "z2181b-qc.test.wdds.zk.com:10.213.43.89"
        "z2181c-qc.test.wdds.zk.com:10.213.43.90"

        // prod kafka zk
        "zk2181a.wdds.zk.com:10.209.33.69"
        "zk2181b.wdds.zk.com:10.209.37.19"
        "zk2181c.wdds.zk.com:10.209.37.68"

        // sit kafka zk
        "z2181a.sit.wdds.zk.com:10.213.33.154"
        "z2181b.sit.wdds.zk.com:10.213.42.48"
        "z2181c.sit.wdds.zk.com:10.213.42.49"

        // test kafka zk
        "z12181a.test.wdds.zk.com:10.213.42.140"
        "z12181b.test.wdds.zk.com:10.213.42.141"
        "z12181c.test.wdds.zk.com:10.213.42.142"

        // test app zk
        "z2181a-app.test.wdds.zk.com:10.213.43.69"
        "z2181b-app.test.wdds.zk.com:10.213.43.70"
        "z2181c-app.test.wdds.zk.com:10.213.43.72"

        // sit app zk
        "z2181a-app.sit.wdds.zk.com:10.213.57.247"
        "z2181b-app.sit.wdds.zk.com:10.213.34.37"
        "z2181c-app.sit.wdds.zk.com:10.213.57.245"

        // prod app zk
        "z2181a-app.wdds.zk.com:10.213.1.225"
        "z2181b-app.wdds.zk.com:10.213.10.140"
        "z2181c-app.wdds.zk.com:10.213.18.207"
        "z2181d-app.wdds.zk.com:10.213.10.145"
        "z2181e-app.wdds.zk.com:10.213.18.215"
        
        // test kafka brokers
        "k10001a.test.wdds.kfk.com:10.213.57.156"
        "k10001b.test.wdds.kfk.com:10.213.42.135"

        // sit kafka brokers
        "k10101a.sit.wdds.kfk.com:10.213.33.148"
        "k10101b.sit.wdds.kfk.com:10.213.33.149"
        "k10102a.sit.wdds.kfk.com:10.213.33.148"
        "k10102b.sit.wdds.kfk.com:10.213.33.149"
        "k10103a.sit.wdds.kfk.com:10.213.33.148"
        "k10103b.sit.wdds.kfk.com:10.213.33.149"
        "k10104a.sit.wdds.kfk.com:10.213.33.148"
        "k10104b.sit.wdds.kfk.com:10.213.33.149"
        "k10105a.sit.wdds.kfk.com:10.213.33.148"
        "k10105b.sit.wdds.kfk.com:10.213.33.149"
        "k10106a.sit.wdds.kfk.com:10.213.33.148"
        "k10106b.sit.wdds.kfk.com:10.213.33.149"
        "k10107a.sit.wdds.kfk.com:10.213.33.148"
        "k10107b.sit.wdds.kfk.com:10.213.33.149"
        "k10108a.sit.wdds.kfk.com:10.213.33.148"
        "k10108b.sit.wdds.kfk.com:10.213.33.149"
        "k10109a.sit.wdds.kfk.com:10.213.33.148"
        "k10109b.sit.wdds.kfk.com:10.213.33.149"
        "k10110a.sit.wdds.kfk.com:10.213.33.148"
        "k10110b.sit.wdds.kfk.com:10.213.33.149"
        "k10111a.sit.wdds.kfk.com:10.213.33.148"
        "k10111b.sit.wdds.kfk.com:10.213.33.149"
        "k10112a.sit.wdds.kfk.com:10.213.33.148"
        "k10112b.sit.wdds.kfk.com:10.213.33.149"
        "k10113a.sit.wdds.kfk.com:10.213.33.148"
        "k10113b.sit.wdds.kfk.com:10.213.33.149"
        "k10114a.sit.wdds.kfk.com:10.213.33.148"
        "k10114b.sit.wdds.kfk.com:10.213.33.149"
        "k10115a.sit.wdds.kfk.com:10.213.33.148"
        "k10115b.sit.wdds.kfk.com:10.213.33.149"
        "k10116a.sit.wdds.kfk.com:10.213.33.148"
        "k10116b.sit.wdds.kfk.com:10.213.33.149"
        "k10117c.sit.wdds.kfk.com:10.213.33.150"
        "k10117d.sit.wdds.kfk.com:10.213.33.151"
        "k10118c.sit.wdds.kfk.com:10.213.33.150"
        "k10118d.sit.wdds.kfk.com:10.213.33.151"
        "k11000a.sit.wdds.kfk.com:10.213.33.148"
        "k11000b.sit.wdds.kfk.com:10.213.33.149"
        "k11001a.sit.wdds.kfk.com:10.213.33.148"
        "k11001b.sit.wdds.kfk.com:10.213.33.149"
        
        // prod kafka brokers
        "k10101a.wdds.kfk.com:10.209.37.39"
        "k10101b.wdds.kfk.com:10.209.33.20"
        "k10102a.wdds.kfk.com:10.209.37.39"
        "k10102b.wdds.kfk.com:10.209.33.20"
        "k10103a.wdds.kfk.com:10.209.37.39"
        "k10103b.wdds.kfk.com:10.209.33.20"
        "k10104a.wdds.kfk.com:10.209.37.39"
        "k10104b.wdds.kfk.com:10.209.33.20"
        "k10105a.wdds.kfk.com:10.209.37.39"
        "k10105b.wdds.kfk.com:10.209.33.20"
        "k10106a.wdds.kfk.com:10.209.37.39"
        "k10106b.wdds.kfk.com:10.209.33.20"
        "k10107a.wdds.kfk.com:10.209.37.39"
        "k10107b.wdds.kfk.com:10.209.33.20"
        "k10108a.wdds.kfk.com:10.209.37.39"
        "k10108b.wdds.kfk.com:10.209.33.20"
        "k10109a.wdds.kfk.com:10.209.37.39"
        "k10109b.wdds.kfk.com:10.209.33.20"
        "k10110a.wdds.kfk.com:10.209.37.39"
        "k10110b.wdds.kfk.com:10.209.33.20"
        "k10111a.wdds.kfk.com:10.209.37.39"
        "k10111b.wdds.kfk.com:10.209.33.20"
        "k10112a.wdds.kfk.com:10.209.37.39"
        "k10112b.wdds.kfk.com:10.209.33.20"
        "k10113a.wdds.kfk.com:10.209.37.69"
        "k10113b.wdds.kfk.com:10.209.33.40"
        "k10114a.wdds.kfk.com:10.209.37.69"
        "k10114b.wdds.kfk.com:10.209.33.40"
        "k10115a.wdds.kfk.com:10.209.37.69"
        "k10115b.wdds.kfk.com:10.209.33.40"
        "k10115c.wdds.kfk.com:10.209.240.191"
        "k10115d.wdds.kfk.com:10.209.240.193"
        "k10116a.wdds.kfk.com:10.209.37.69"
        "k10116b.wdds.kfk.com:10.209.33.40"
        "k10117a.wdds.kfk.com:10.209.37.69"
        "k10117b.wdds.kfk.com:10.209.33.40"
        "k10118a.wdds.kfk.com:10.209.11.166"
        "k10118b.wdds.kfk.com:10.209.11.195"
        "k10119a.wdds.kfk.com:10.209.37.69"
        "k10119b.wdds.kfk.com:10.209.33.40"
        "k10120a.wdds.kfk.com:10.209.10.161"
        "k10120b.wdds.kfk.com:10.209.10.141"
        "k10121a.wdds.kfk.com:10.209.19.143"
        "k10121b.wdds.kfk.com:10.209.19.144"
        "k10122a.wdds.kfk.com:10.209.37.69"
        "k10122b.wdds.kfk.com:10.209.33.40"
        "k11000a.wdds.kfk.com:10.209.20.194"
        "k11000b.wdds.kfk.com:10.209.18.16"
        "k11001a.wdds.kfk.com:10.209.37.69"
        "k11001b.wdds.kfk.com:10.209.33.40"
        "k11003a.wdds.kfk.com:10.209.20.194"
        "k11003b.wdds.kfk.com:10.209.18.16"       
        "k11006a.wdds.kfk.com:10.209.19.143"
        "k11006b.wdds.kfk.com:10.209.19.144"
    ]    
}
`
)
