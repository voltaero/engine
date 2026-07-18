use enginelib::api::ServerAPI;

fn main() {
    let mut api = ServerAPI::default();
    let server = enginelib::server::RPC::new(&mut api);
    server.run();
}
