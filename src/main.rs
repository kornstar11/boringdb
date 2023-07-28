use boringdb::ServerFactory;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    env_logger::init();
    log::info!("Starting...");
    let server = ServerFactory {
        addr: "0.0.0.0:6379".parse().unwrap(),
        outstanding_requests: 10000,
    };
    server.start().unwrap();
}
