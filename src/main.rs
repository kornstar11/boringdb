use boringdb::ServerFactory;

fn main() {
    env_logger::init();
    log::info!("Starting...");
    let server = ServerFactory {
        addr: "0.0.0.0:6379".parse().unwrap(),
        outstanding_requests: 1000,
    };
    server.start().unwrap();
}
