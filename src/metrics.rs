// All metrics code is temporarily disabled to focus on core compilation errors.
use crate::protocol::mc::Tracker;

pub async fn init(_port: u16) -> std::io::Result<()> {
    // Do nothing.
    Ok(())
}

pub fn front_conn_incr() {}
pub fn front_conn_decr() {}
pub fn cluster_cmd_total(_cluster: &str, _cmd: &str) {}
pub fn cluster_cmd_remote(_cluster: &str, _cmd: &str) {}
pub fn total_tracker(_cluster: &str) -> Tracker {
    Tracker::new()
}
pub fn remote_tracker(_cluster: &str) -> Tracker {
    Tracker::new()
}
