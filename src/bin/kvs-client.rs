use clap::arg_enum;
use kvs::engines::KvEngine;
use kvs::engines::SledKvsEngine;
use kvs::*;
use log::LevelFilter;
use log::{error, info, warn};
use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::exit;
use structopt::StructOpt;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-server")]
struct Opt {
    #[structopt(
        long,
        help = "Sets the listening address",
        value_name = "addr",
        default_value = DEFAULT_LISTENING_ADDRESS,
        parse(try_from_str)
    )]
    addr: SocketAddr,
    #[structopt(
        long,
        help = "Sets the storage engine",
        value_name = "engine",
        possible_values = &Engine::variants()
    )]
    engine: Option<Engine>,
}

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum Engine {
        kvs,
        sled
    }
}

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let mut opt = Opt::from_args();
    let res = current_engine().and_then(move |curr_engine| {
        if opt.engine.is_none() {
            opt.engine = curr_engine;
        }
        if curr_engine.is_some() && opt.engine != curr_engine {
            error!("Wrong engine!");
            exit(1);
        }
        run(opt)
    });
    if let Err(e) = res {
        error!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);
    info!("kv-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", opt.addr);
    info!("Current dir: {:?}", current_dir()?.as_mut_os_string());
    let data_dir = "/tmp/";
    fs::write(current_dir()?.join("engine"), format!("{}", engine))?;
    match engine {
        Engine::kvs => run_with_engine(
            KvStore::open(PathBuf::from(format!("{}{}", data_dir, "kvs/kvs")))?,
            opt.addr,
        ),
        Engine::sled => run_with_engine(
            SledKvsEngine::new(sled::open(PathBuf::from(format!(
                "{}{}",
                data_dir, "sled/seld"
            )))?),
            opt.addr,
        ),
    }
}
fn run_with_engine<E: KvEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let server = KvServer::new(engine);
    server.run(addr)
}

fn current_engine() -> Result<Option<Engine>> {
    let engine = current_dir()?.join("engine");
    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}
