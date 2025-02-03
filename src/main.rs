mod common;
mod logging;
mod subcommands;
pub(crate) mod test_utils;

use std::{fs::OpenOptions, process};

use clap::{crate_description, crate_name, crate_version, Arg, Command};
use log::error;

use subcommands::{
    archive, check, execution_results_summary, extract_slice, latest_block_summary,
    purge_signatures, remove_block, trie_compact, unsparse, Error,
};

const LOGGING: &str = "logging";

enum DisplayOrder {
    Archive,
    Check,
    ExecutionResults,
    ExtractSlice,
    LatestBlock,
    PurgeSignatures,
    RemoveBlock,
    TrieCompact,
    Unsparse,
}

const VERSION_STRING: &str = concat!(
    crate_version!(),
    "\n",
    "This version of ",
    crate_name!(),
    " is compatible with casper-node version ",
    env!("CASPER_NODE_VERSION")
);

fn cli() -> Command<'static> {
    Command::new("casper-db-utils")
        .version(VERSION_STRING)
        .about(crate_description!())
        .arg_required_else_help(true)
        .subcommand(archive::command(DisplayOrder::Archive as usize))
        .subcommand(check::command(DisplayOrder::Check as usize))
        .subcommand(execution_results_summary::command(
            DisplayOrder::ExecutionResults as usize,
        ))
        .subcommand(extract_slice::command(DisplayOrder::ExtractSlice as usize))
        .subcommand(latest_block_summary::command(
            DisplayOrder::LatestBlock as usize,
        ))
        .subcommand(purge_signatures::command(
            DisplayOrder::PurgeSignatures as usize,
        ))
        .subcommand(remove_block::command(DisplayOrder::RemoveBlock as usize))
        .subcommand(trie_compact::command(DisplayOrder::TrieCompact as usize))
        .subcommand(unsparse::command(DisplayOrder::Unsparse as usize))
        .arg(
            Arg::new(LOGGING)
                .short('l')
                .long(LOGGING)
                .takes_value(true)
                .value_name("LOGFILE_PATH")
                .help("Path to file where program will dump log messages."),
        )
}

fn main() {
    let arg_matches = cli().get_matches();

    // Initialize logger.
    arg_matches.value_of(LOGGING).map_or_else(
        || logging::init_term_logger().expect("Couldn't initialize terminal logger"),
        |path| {
            let logfile = OpenOptions::new()
                .append(true)
                .create(true)
                .open(path)
                .expect("Couldn't open logfile");
            let line_writer = std::io::LineWriter::new(logfile);
            logging::init_write_logger(line_writer).expect("Couldn't initialize logger to file");
        },
    );

    let (subcommand_name, matches) = arg_matches.subcommand().unwrap_or_else(|| {
        error!(
            "{}",
            cli().get_long_about().expect("should have long about")
        );
        process::exit(1);
    });

    let result: Result<(), Error> = match subcommand_name {
        archive::COMMAND_NAME => archive::run(matches).map_err(Error::from),
        check::COMMAND_NAME => check::run(matches).map_err(Error::from),
        execution_results_summary::COMMAND_NAME => {
            execution_results_summary::run(matches).map_err(Error::from)
        }
        extract_slice::COMMAND_NAME => extract_slice::run(matches).map_err(Error::from),
        latest_block_summary::COMMAND_NAME => {
            latest_block_summary::run(matches).map_err(Error::from)
        }
        purge_signatures::COMMAND_NAME => purge_signatures::run(matches).map_err(Error::from),
        remove_block::COMMAND_NAME => remove_block::run(matches).map_err(Error::from),
        trie_compact::COMMAND_NAME => trie_compact::run(matches).map_err(Error::from),
        unsparse::COMMAND_NAME => unsparse::run(matches).map_err(Error::from),
        _ => unreachable!("{} should be handled above", subcommand_name),
    };

    if let Err(run_err) = result {
        error!("{}", run_err);
        process::exit(1);
    }
}
