## Here is an investigation pet-project of Jaeger

### Base structure:

```text
┌─────────────┐  Kafka  ┌─────────────┐  HTTP  ┌─────────────┐        ┌──────────────┐
│  Producer   │────────▶│  Consumer   │───────▶│  Storage    │───────▶│  PostgreSQL  │
└─────────────┘         └─────────────┘        └─────────────┘        └──────────────┘
       │                      │                      │
       ▼                      ▼                      ▼
┌────────────────────────────────────────────────────────────┐
│                           Jaeger                           │
└────────────────────────────────────────────────────────────┘
```

Test API request:
```text
    curl -X POST http://localhost:8080/data -d '{"test":"data"}'
```

Jaeger dashboard:
```text
  http://localhost:16686
```

Basic commands:
```text
    docker-compose up -d --build
    docker-compose down -v  
```

Other useful commands:
```text
    docker ps -a | grep jaeger 
    docker network inspect tracing-network -f '{{range .Containers}}{{.Name}} {{end}}'
    docker-compose exec producer-service nc -zuv jaeger 6831
    docker-compose exec producer-service nslookup jaeger 
    docker-compose exec producer-service ping jaeger
    docker network rm tracing-network 2>/dev/null || true 
```
