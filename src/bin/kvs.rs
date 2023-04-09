use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvStore, KvsErr, Result, engines::KvEngine};

use std::{env::current_dir, process::exit};
fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(
                    Arg::with_name("VALUE")
                        .help("The string value of the key")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .get_matches();

    match matches.subcommand() {
        ("set", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();
            let value = _matches.value_of("VALUE").unwrap();
            let mut kv_store = KvStore::open(current_dir()?)?;

            kv_store.set(key.to_string(), value.to_string())
        }
        ("get", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();

            let mut kv_store = KvStore::open(current_dir()?)?;
            if let Some(value) = kv_store.get(key.to_string())? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
            Ok(())

        }
        ("rm", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key.to_string()) {
                Ok(()) => Ok({}),
                Err(KvsErr::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            }
        }
        _ => unreachable!(),
    }
}
