# MapReduce in Go

Un semplice esempio di applicazione distribuita per ordinare dataset utilizzando MapReduce.

---

## Istruzioni

### 1) Crea il modulo Go
```bash
$ go mod init mapReduce
$ go mod tidy
```

### 2) (Facoltativo) Compila i file di definizione gRPC (se usi gRPC)
```bash
$ protoc --go_out=. --go_opt=paths=source_relative \
         --go-grpc_out=. --go-grpc_opt=paths=source_relative \
         mapreduce/mapreduce.proto
```

### 3) Esegui il programma
```bash
$ go run main.go
```

---

Esegui il programma per avviare il sistema di MapReduce. Verranno simulati i nodi Master, Mapper e Reducer sulla stessa macchina, e i risultati saranno salvati su file.
