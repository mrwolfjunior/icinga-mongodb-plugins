# check_mongodb — Icinga Plugin per MongoDB

Plugin monolitico Python per il monitoraggio di istanze MongoDB on-prem via Icinga/Nagios.

Supporta: **Single Node**, **ReplicaSet**, **ReplicaSet con Arbiter**, **Sharded Cluster**.

## Requisiti

| Requisito | Versione |
|---|---|
| Python | ≥ 3.8 |
| pymongo | ≥ 4.0, < 5.0 |
| dnspython | ≥ 2.0 |
| MongoDB | 5.x — 8.2 |

### Installazione

```bash
pip install -r requirements.txt
```

Per lo sviluppo e i test:

```bash
pip install -r requirements-dev.txt
```

## Modalità di Check

Lo script supporta **3 modalità** mutuamente esclusive:

### `--availability` — Disponibilità nodi

Verifica che tutti i nodi siano raggiungibili e in stato sano.

- **Check diretto**: connessione diretta a ogni nodo dichiarato nella URI
- **Check indiretto**: interroga `replSetGetStatus` per confrontare lo stato visto dal cluster vs lo stato visto da Icinga
- **Validazione**: verifica che il nome del ReplicaSet corrisponda a quello nella URI
- **Quorum**: verifica che la majority sia rispettata per le scritture
- **Arbiter**: se l'arbiter non è raggiungibile direttamente ma rsStatus dice che è sano → OK (può essere su rete segregata)

**Severity**:
- Qualsiasi nodo data-bearing giù → **CRITICAL**
- Nome RS non corrispondente → **CRITICAL**
- Nodo raggiungibile ma RS lo vede in stato anomalo → **CRITICAL**
- Quorum perso → **CRITICAL**

```bash
# ReplicaSet
./check_mongodb.py --uri "mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myRS" \
    --availability --username admin --password secret

# Sharded Cluster
./check_mongodb.py --uri "mongodb://mongos1:27017,mongos2:27017/" \
    --availability --tls
```

### `--metrics` — Metriche prestazionali

Connessione DIRETTA nodo per nodo per raccogliere metriche da `serverStatus`.

| Metrica | Soglia applicabile via `--thresholds` |
|---|---|
| Connection usage % | `conn_usage_pct` (above) |
| Replication lag | `repl_lag` (above, secondi) |
| Oplog window | `oplog_window` (below, ore) |
| WiredTiger cache usage % | `cache_usage_pct` (above) |
| WiredTiger tickets read/write % | `tickets_read_pct`, `tickets_write_pct` (above) |
| Queue total | `queue_total` (above) |
| Cursors open / timed out | `cursor_open`, `cursor_timed_out` (above) |
| Assertions | `assertions_regular`, `assertions_warning` (above) |
| Operations/sec, memory, network, document ops, page faults, active clients, database sizes, filesystem, oplog size, transactions | Solo perfdata |

> **⚠️ Oplog window**: le soglie oplog sono **invertite** (mode `below`): l'allarme scatta quando la finestra scende **sotto** il valore configurato.

> **⚠️ WiredTiger tickets**: in MongoDB 7.0+ il pool è dinamico (max 128). Le soglie usano la **% di utilizzo** così funzionano sia con pool fisso che dinamico.

```bash
./check_mongodb.py --uri "mongodb://host1:27017,host2:27017/?replicaSet=rs0" \
    --metrics --thresholds '{
      "conn_usage_pct": {"warning": 80, "critical": 90},
      "oplog_window":   {"warning": 48, "critical": 24, "mode": "below"},
      "tickets_read_pct":  {"warning": 80, "critical": 95},
      "tickets_write_pct": {"warning": 80, "critical": 95},
      "repl_lag":        {"warning": 30, "critical": 120}
    }'
```

### `--filesystem` — Occupazione filesystem

Controlla lo spazio disco tramite `dbStats` (`fsTotalSize` / `fsUsedSize`).

**Soglie dinamiche (formula logaritmica)**: su volumi grandi la soglia percentuale viene automaticamente adattata per richiedere più spazio libero in termini assoluti.

| Volume | Soglia base 90% | Spazio libero richiesto |
|---|---|---|
| 100 GB | 90% | 10 GB |
| 1 TB | ~93% | ~70 GB |
| 5 TB | ~95% | ~250 GB |
| 10 TB | ~96% | ~400 GB |

```bash
./check_mongodb.py --uri "mongodb://host1:27017,host2:27017/" \
    --filesystem --thresholds '{"fs_usage_pct": {"warning": 85, "critical": 95}}'
```

## Parametri

| Parametro | Descrizione | Default |
|---|---|---|
| `--uri` | Connection string MongoDB | **obbligatorio** |
| `--username`, `-u` | Username autenticazione | — |
| `--password`, `-p` | Password autenticazione | — |
| `--auth-mechanism` | `SCRAM-SHA-256`, `SCRAM-SHA-1`, `PLAIN` (LDAP) | auto |
| `--auth-source` | Database autenticazione | `admin` (`$external` per LDAP) |
| `--tls` | Abilita TLS/SSL | `false` |
| `--tls-insecure` | Disabilita verifica certificato TLS | `false` |
| `--timeout` | Timeout connessione (secondi) | `10` |
| `--thresholds` | JSON con soglie per metrica (vedi sopra) | — |
| `--replicaset` | Nome RS atteso (override URI) | — |
| `--verbose`, `-v` | Output verboso per debug | `false` |

## Exit Codes

| Codice | Stato | Descrizione |
|---|---|---|
| 0 | OK | Tutto funziona correttamente |
| 1 | WARNING | Soglia warning superata |
| 2 | CRITICAL | Nodo/i giù, soglia critical superata, errore grave |
| 3 | UNKNOWN | Errore plugin o check non supportato |

## Performance Data

L'output include perfdata nel formato standard Nagios:

```
STATUS - message | label=value[UOM];warn;crit;min;max
```

I label usano il formato `<host>_<port>_<metrica>` per identificare univocamente ogni metrica per nodo.

## Configurazione Icinga2

Esempio di `CheckCommand`:

```
object CheckCommand "mongodb" {
    command = [ PluginDir + "/check_mongodb.py" ]

    arguments = {
        "--uri" = "$mongodb_uri$"
        "--username" = {
            value = "$mongodb_username$"
            set_if = {{ macro("$mongodb_username$") != false }}
        }
        "--password" = {
            value = "$mongodb_password$"
            set_if = {{ macro("$mongodb_password$") != false }}
        }
        "--auth-mechanism" = {
            value = "$mongodb_auth_mechanism$"
            set_if = {{ macro("$mongodb_auth_mechanism$") != false }}
        }
        "--tls" = {
            set_if = "$mongodb_tls$"
        }
        "--availability" = {
            set_if = "$mongodb_check_availability$"
        }
        "--metrics" = {
            set_if = "$mongodb_check_metrics$"
        }
        "--filesystem" = {
            set_if = "$mongodb_check_filesystem$"
        }

        "--timeout" = {
            value = "$mongodb_timeout$"
            set_if = {{ macro("$mongodb_timeout$") != false }}
        }
        "--replicaset" = {
            value = "$mongodb_replicaset$"
            set_if = {{ macro("$mongodb_replicaset$") != false }}
        }
        "--verbose" = {
            set_if = "$mongodb_verbose$"
        }
        "--thresholds" = {
            value = "$mongodb_thresholds$"
            set_if = {{ macro("$mongodb_thresholds$") != false }}
        }
    }
}
```

Esempio di `Service`:

```
apply Service "mongodb-availability" {
    check_command = "mongodb"
    vars.mongodb_uri = "mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myRS"
    vars.mongodb_check_availability = true
    vars.mongodb_username = "icinga"
    vars.mongodb_password = "secret"
    vars.mongodb_tls = true
    check_interval = 1m
    retry_interval = 30s
    assign where host.vars.role == "mongodb"
}
```

## Testing

### Unit Tests

```bash
python -m pytest tests/test_check_mongodb.py -v
```

### End-to-End Tests (richiede Docker)

```bash
# Avvia ambiente di test
docker compose -f docker/docker-compose.replicaset.yml up -d
sleep 30

# Esegui test (metodo consigliato per evitare problemi DNS)
./run_e2e.sh -v

# Esegui un singolo test
./run_e2e.sh -v -k test_availability_quorum_lost

# Cleanup
docker compose -f docker/docker-compose.replicaset.yml down -v
```

### Fault Injection

```bash
# Ferma un nodo
./tests/fault_injection.sh stop-node mongo2

# Simula partizione di rete
./tests/fault_injection.sh network-partition mongo2

# Ripristina
./tests/fault_injection.sh start-node mongo2
./tests/fault_injection.sh restore-network mongo2
```

## Ambienti Docker per Test

| File | Topologia |
|---|---|
| `docker/docker-compose.single.yml` | Single node |
| `docker/docker-compose.replicaset.yml` | ReplicaSet 3 nodi |
| `docker/docker-compose.replicaset-arbiter.yml` | ReplicaSet 2 data + 1 arbiter |
| `docker/docker-compose.sharded.yml` | Sharded cluster completo |

## Licenza

MIT
