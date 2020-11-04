package ink.casual.prometeus.exporter.opentsdb;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liuwentao
 * @date 2020/11/4
 */
@RestController
public class MetricsController {

    private static Map<String, Map<String, Metrics>> metricsMap = new HashMap<>();

    @Value("${opentsdb.url:localhost:4242}")
    private String url;

    @PostConstruct
    public void init() {
        // 官方/api/stats/接口提供的metric 注释不需要的
        String METRIC_INIT_STRING = "tsd.connectionmgr.connections,type=open,Gauge,The number of currently open Telnet and HTTP connections.\n" +
//                "tsd.connectionmgr.connections,type=total,Counter,The total number of connections made to OpenTSDB. This includes all Telnet and HTTP connections.\n" +
                "tsd.connectionmgr.exceptions,type=closed,Counter,The total number of exceptions caused by writes to a channel that was already closed. This can occur if a query takes too long the client closes their connection gracefully and the TSD attempts to write to the socket. This includes all Telnet and HTTP connections.\n" +
                "tsd.connectionmgr.exceptions,type=reset,Counter,The total number of exceptions caused by a client disconnecting without closing the socket. This includes all Telnet and HTTP connections.\n" +
                "tsd.connectionmgr.exceptions,type=timeout,Counter,The total exceptions caused by a socket inactivity timeout i.e. the TSD neither wrote nor received data from a socket within the timeout period. This includes all Telnet and HTTP connections.\n" +
                "tsd.connectionmgr.exceptions,type=unknown,Counter,The total exceptions with an unknown cause. Check the logs for details. This includes all Telnet and HTTP connections.\n" +
//                "tsd.rpc.received,type=telnet,Counter,The total number of telnet RPC requests received\n" +
//                "tsd.rpc.received,type=http,Counter,The total number of Http RPC requests received\n" +
//                "tsd.rpc.received,type=http_plugin,Counter,The total number of Http RPC requests received and handled by a plugin instead of the built-in APIs. (v2.2)\n" +
                "tsd.rpc.exceptions,,Counter,The total number exceptions caught during RPC calls. These may be user error or bugs.\n" +
//                "tsd.http.latency_50pct,type=all,Gauge,The time it took in milliseconds to answer HTTP requests for the 50th percentile cases\n" +
//                "tsd.http.latency_75pct,type=all,Gauge,The time it took in milliseconds to answer HTTP requests for the 75th percentile cases\n" +
//                "tsd.http.latency_90pct,type=all,Gauge,The time it took in milliseconds to answer HTTP requests for the 90th percentile cases\n" +
                "tsd.http.latency_95pct,type=all,Gauge,The time it took in milliseconds to answer HTTP requests for the 95th percentile cases\n" +
//                "tsd.http.latency_50pct,type=graph,Gauge,The time it took in milliseconds to answer graphing requests for the 50th percentile cases\n" +
//                "tsd.http.latency_75pct,type=graph,Gauge,The time it took in milliseconds to answer graphing requests for the 75th percentile cases\n" +
//                "tsd.http.latency_90pct,type=graph,Gauge,The time it took in milliseconds to answer graphing requests for the 90th percentile cases\n" +
//                "tsd.http.latency_95pct,type=graph,Gauge,The time it took in milliseconds to answer graphing requests for the 95th percentile cases\n" +
//                "tsd.http.latency_50pct,type=gnuplot,Gauge,The time it took in milliseconds to generate the GnuPlot graphs for the 50th percentile cases\n" +
//                "tsd.http.latency_75pct,type=gnuplot,Gauge,The time it took in milliseconds to generate the GnuPlot graphs for the 75th percentile cases\n" +
//                "tsd.http.latency_90pct,type=gnuplot,Gauge,The time it took in milliseconds to generate the GnuPlot graphs for the 90th percentile cases\n" +
//                "tsd.http.latency_95pct,type=gnuplot,Gauge,The time it took in milliseconds to generate the GnuPlot graphs for the 95th percentile cases\n" +
//                "tsd.http.graph.requests,cache=disk,Counter,The total number of graph requests satisfied from the disk cache\n" +
//                "tsd.http.graph.requests,cache=miss,Counter,The total number of graph requests that were not cached and required a fetch from storage\n" +
                "tsd.http.query.invalid_requests,,Counter,The total number data queries sent to the /api/query endpoint that were invalid due to user errors such as using the wrong HTTP method missing parameters or using metrics and tags without UIDs. (v2.2)\n" +
                "tsd.http.query.exceptions,,Counter,The total number data queries sent to the /api/query endpoint that threw an exception due to bad user input or an underlying error. See logs for details. (v2.2)\n" +
                "tsd.http.query.success,,Counter,The total number data queries sent to the /api/query endpoint that completed successfully. Note that these may have returned an empty result. (v2.2)\n" +
//                "tsd.rpc.received,type=put,Counter,The total number of put requests for writing data points\n" +
                "tsd.rpc.errors,type=hbase_errors,Counter,The total number of RPC errors caused by HBase exceptions\n" +
                "tsd.rpc.errors,type=invalid_values,Counter,The total number of RPC errors caused invalid put values from user requests such as a string instead of a number\n" +
                "tsd.rpc.errors,type=illegal_arguments,Counter,The total number of RPC errors caused by bad data from the user\n" +
                "tsd.rpc.errors,type=socket_writes_blocked,Counter,The total number of times the TSD was unable to write back to the telnet socket due to a full buffer. If this happens it likely means a number of exceptions were happening. (v2.2)\n" +
                "tsd.rpc.errors,type=unknown_metrics,Counter,The total number of RPC errors caused by attempts to put a metric without an assigned UID. This only increments if auto metrics is disabled.\n" +
                "tsd.uid.cache-hit,kind=metrics,Counter,The total number of successful cache lookups for metric UIDs\n" +
                "tsd.uid.cache-miss,kind=metrics,Counter,The total number of failed cache lookups for metric UIDs that required a call to storage\n" +
//                "tsd.uid.cache-size,kind=metrics,Gauge,The current number of cached metric UIDs\n" +
//                "tsd.uid.ids-used,kind=metrics,Counter,The current number of assigned metric UIDs. (NOTE: if random metric UID generation is enabled ids-used will always be 0)\n" +
//                "tsd.uid.ids-available,kind=metrics,Counter,The current number of available metric UIDs decrements as UIDs are assigned. (NOTE: if random metric UID generation is enabled ids-used will always be 0)\n" +
//                "tsd.uid.random-collisions,kind=metrics,Counter,How many times metric UIDs attempted a reassignment due to a collision with an existing UID. (v2.2)\n" +
                "tsd.uid.cache-hit,kind=tagk,Counter,The total number of successful cache lookups for tagk UIDs\n" +
                "tsd.uid.cache-miss,kind=tagk,Counter,The total number of failed cache lookups for tagk UIDs that required a call to storage\n" +
//                "tsd.uid.cache-size,kind=tagk,Gauge,The current number of cached tagk UIDs\n" +
//                "tsd.uid.ids-used,kind=tagk,Counter,The current number of assigned tagk UIDs\n" +
//                "tsd.uid.ids-available,kind=tagk,Counter,The current number of available tagk UIDs decrements as UIDs are assigned.\n" +
                "tsd.uid.cache-hit,kind=tagv,Counter,The total number of successful cache lookups for tagv UIDs\n" +
                "tsd.uid.cache-miss,kind=tagv,Counter,The total number of failed cache lookups for tagv UIDs that required a call to storage\n" +
//                "tsd.uid.cache-size,kind=tagv,Gauge,The current number of cached tagv UIDs\n" +
//                "tsd.uid.ids-used,kind=tagv,Counter,The current number of assigned tagv UIDs\n" +
//                "tsd.uid.ids-available,kind=tagv,Counter,The current number of available tagv UIDs decrements as UIDs are assigned.\n" +
                "tsd.jvm.ramfree,,Gauge,The number of bytes reported as free by the JVM’s Runtime.freeMemory()\n" +
                "tsd.jvm.ramused,,Gauge,The number of bytes reported as used by the JVM’s Runtime.totalMemory()\n" +
//                "tsd.hbase.latency_50pct,method=put,Gauge,The time it took in milliseconds to execute a Put call for the 50th percentile cases\n" +
//                "tsd.hbase.latency_75pct,method=put,Gauge,The time it took in milliseconds to execute a Put call for the 75th percentile cases\n" +
//                "tsd.hbase.latency_90pct,method=put,Gauge,The time it took in milliseconds to execute a Put call for the 90th percentile cases\n" +
//                "tsd.hbase.latency_95pct,method=put,Gauge,The time it took in milliseconds to execute a Put call for the 95th percentile cases\n" +
//                "tsd.hbase.latency_50pct,method=scan,Gauge,The time it took in milliseconds to execute a Scan call for the 50th percentile cases\n" +
//                "tsd.hbase.latency_75pct,method=scan,Gauge,The time it took in milliseconds to execute a Scan call for the 75th percentile cases\n" +
//                "tsd.hbase.latency_90pct,method=scan,Gauge,The time it took in milliseconds to execute a Scan call for the 90th percentile cases\n" +
                "tsd.hbase.latency_95pct,method=scan,Gauge,The time it took in milliseconds to execute a Scan call for the 95th percentile cases\n" +
//                "tsd.hbase.root_lookups,,Counter,The total number of root lookups performed by the client\n" +
//                "tsd.hbase.meta_lookups,type=uncontended,Counter,The total number of uncontended meta table lookups performed by the client\n" +
//                "tsd.hbase.meta_lookups,type=contended,Counter,The total number of contended meta table lookups performed by the client\n" +
                "tsd.hbase.rpcs,type=increment,Counter,The total number of Increment requests performed by the client\n" +
                "tsd.hbase.rpcs,type=delete,Counter,The total number of Delete requests performed by the client\n" +
                "tsd.hbase.rpcs,type=get,Counter,The total number of Get requests performed by the client\n" +
                "tsd.hbase.rpcs,type=put,Counter,The total number of Put requests performed by the client\n" +
                "tsd.hbase.rpcs,type=rowLock,Counter,The total number of Row Lock requests performed by the client\n" +
                "tsd.hbase.rpcs,type=openScanner,Counter,The total number of Open Scanner requests performed by the client\n" +
                "tsd.hbase.rpcs,type=scan,Counter,The total number of Scan requests performed by the client. These indicate a scan->next() call.\n" +
                "tsd.hbase.rpcs.batched,,Counter,The total number of batched requests sent by the client\n" +
//                "tsd.hbase.flushes,,Counter,The total number of flushes performed by the client\n" +
//                "tsd.hbase.connections.created,,Counter,The total number of connections made by the client to region servers\n" +
//                "tsd.hbase.nsre,,Counter,The total number of No Such Region Exceptions caught. These can happen when a region server crashes is taken offline or when a region splits (?)\n" +
//                "tsd.hbase.nsre.rpcs_delayed,,Counter,The total number of calls delayed due to an NSRE that were later successfully executed\n" +
//                "tsd.hbase.region_clients.open,,Counter,The total number of connections opened to region servers since the TSD started. If this number is climbing the region servers may be crashing and restarting. (v2.2)\n" +
//                "tsd.hbase.region_clients.idle_closed,,Counter,The total number of connections to region servers that were closed due to idle connections. This indicates nothing was read from or written to a server in some time and the TSD will reconnect when it needs to. (v2.2)\n" +
//                "tsd.compaction.count,type=trivial,Counter,The total number of trivial compactions performed by the TSD\n" +
//                "tsd.compaction.count,type=complex,Counter,The total number of complex compactions performed by the TSD\n" +
//                "tsd.compaction.duplicates,type=identical,Counter,The total number of data points found during compaction that were duplicates at the same time and with the same value. (v2.2)\n" +
//                "tsd.compaction.duplicates,type=variant,Counter,The total number of data points found during compaction that were duplicates at the same time but with a different value. (v2.2)\n" +
//                "tsd.compaction.queue.size,,Gauge,How many rows of data are currently in the queue to be compacted. (v2.2)\n" +
                "tsd.compaction.errors,rpc=read,Counter,The total number of rows that couldn’t be read from storage due to an error of some sort. (v2.2)\n" +
                "tsd.compaction.errors,rpc=put,Counter,The total number of rows that couldn’t be re-written to storage due to an error of some sort. (v2.2)\n" +
                "tsd.compaction.errors,rpc=delete,Counter,The total number of rows that couldn’t have the old non-compacted data deleted from storage due to an error of some sort. (v2.2)\n" +
                "tsd.compaction.writes,rpc=read,Counter,The total number of writes back to storage of compacted values. (v2.2)\n" +
                "tsd.compaction.deletes,,Counter,The total number of delete calls made to storage to remove old data that has been compacted. (v2.2)";
        String[] metricsInit = METRIC_INIT_STRING.split("\n");
        for (String metricsStr : metricsInit) {
            String[] split = metricsStr.split(",");
            Metrics metrics = new Metrics();
            metrics.setMetric(split[0]);
            metrics.setType(split[2].toLowerCase());
            metrics.setDes(split[3]);
            Map<String, Metrics> map = metricsMap.get(split[0]);
            if (map == null) {
                map = new HashMap<>();
            }
            map.put(Strings.isBlank(split[1]) ? "*" : split[1], metrics);
            metricsMap.put(split[0], map);
        }
    }

    @GetMapping("metrics")
    public void metrics(HttpServletResponse response) throws IOException {
        String result = OkHttpUtil.get(url + "/api/stats");
        List<Metrics> metrics = JSONObject.parseObject(result, new TypeReference<List<Metrics>>() {
        });
        if (CollectionUtils.isEmpty(metrics)) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (Metrics metric : metrics) {
            Metrics metricsInit = null;
            for (Map.Entry<String, String> entry : metric.getTags().entrySet()) {
                Map<String, Metrics> map = metricsMap.get(metric.getMetric());
                if (map == null) {
                    continue;
                }
                metricsInit = map.get(entry.getKey() + "=" + entry.getValue());
                if (metricsInit == null) {
                    metricsInit = map.get("*");
                }
                if (metricsInit != null) {
                    break;
                }
            }
            if (metricsInit != null) {
                metric.setMetric(metric.getMetric().replaceAll("\\.", "_").replaceAll("-", "_"));
                metric.setType(metricsInit.getType());
                metric.setDes(metricsInit.getDes());
                sb.append(metric.toString());
            }
        }
        Metrics active = new Metrics();
        active.setMetric("tsd_active");
        active.setType("gauge");
        active.setDes("1 active");
        active.setValue("1");
        sb.append(active.toString());
        ServletOutputStream outputStream = response.getOutputStream();
        outputStream.write(sb.toString().getBytes());
        outputStream.flush();
    }

    static class Metrics {

        private String metric;
        private String value;
        private String type;
        private Map<String, String> tags;
        private String des;

        public String getMetric() {
            return metric;
        }

        public void setMetric(String metric) {
            this.metric = metric;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, String> getTags() {
            return tags;
        }

        public void setTags(Map<String, String> tags) {
            this.tags = tags;
        }

        public String getDes() {
            return des;
        }

        public void setDes(String des) {
            this.des = des;
        }

        @Override
        public String toString() {
            String help = "# HELP %s %s";
            String type = "# TYPE %s %s";
            String metric = "%s%s %s";
            return String.format(help, this.metric, des) + System.lineSeparator() +
                    String.format(type, this.metric, this.type) + System.lineSeparator() +
                    String.format(metric, this.metric, tagString(), value) + System.lineSeparator();
        }

        private String tagString() {
            if (CollectionUtils.isEmpty(tags)) {
                return "";
            }
            StringBuilder sb = new StringBuilder("{");
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                sb.append(entry.getKey()).append("=").append("\"").append(entry.getValue()).append("\"").append(",");
            }
            String s = sb.toString();
            return s.substring(0, s.length() - 1) + "}";
        }

    }

}
