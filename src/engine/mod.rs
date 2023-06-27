use std::path::PathBuf;

mod database;

///
/// Commands used between the database and compactor
enum InternalCommands {
    NewSSTable(PathBuf),
    DeleteSSTable(PathBuf)

}