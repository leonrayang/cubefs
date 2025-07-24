module github.com/cubefs/cubefs/client_gofuse

go 1.17

require (
	github.com/cubefs/cubefs v1.34.0
	github.com/cubefs/cubefs/sdk_gofuse v0.0.0-00010101000000-000000000000
	github.com/hanwen/go-fuse/v2 v2.1.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.10.0 // indirect
	github.com/bits-and-blooms/bloom/v3 v3.6.0 // indirect
	github.com/brahma-adshonor/gohook v1.1.9 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-ping/ping v1.1.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/jacobsa/daemonize v0.0.0-20160101105449-e460293e890f // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/prometheus/client_golang v1.13.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/shirou/gopsutil v3.21.11+incompatible // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	golang.org/x/arch v0.0.0-20190312162104-788fe5ffcd8c // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.7.0 // indirect
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
)

replace (
	github.com/cubefs/cubefs => ../
	github.com/cubefs/cubefs/sdk_gofuse => ../sdk_gofuse
)
