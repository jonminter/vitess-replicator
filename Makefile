download_proto:
	./scripts/download_vtgate_proto.sh

run_kafka:
	cd infra/kafka && ./run_kafka.sh

run_vitess:
	cd infra/vitess && ./run_vitess.sh

vitess_pf:
	 cd infra/vitess && ./pf.sh

run:
	RUST_LOG=info RUST_BACKTRACE=1 cargo run -- --keyspace commerce --vtctld-endpoint "http://127.0.0.1:15999" --vtgate-endpoint "http://[::]:15099" --tables foo --tables bar
