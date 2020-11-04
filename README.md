### How to run

```
mvn package

java -jar -Dopentsdb.url=ip:port -Dserver.port=8899 opentsdb_exporter.jar &

modify prometheus.yml
 - job_name: "opentsdb"
    static_configs:
    - targets:
      - "ip:8899"

import grafana dashborad
```

![Image text](dashboard.png)
