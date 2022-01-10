# Two-Phase Commit

## Goal

The goal is to use Rust to implement a simple 2PC(two-phase commit) protocol that works with a single coordinator, an arbitrary number of concurrent clients(issuing requests to the coordinator) and an arbitrary number of 2PC participants with the following assumptions:

- The coordinator does not fail.
- Participants may fail either by explicitly voting abort in phase one, or by not responding to requests. Failed participants are allowed to rejoin silently without any recovery protocol involved.
- Requests can be represented by a single operation ID.

## Requirements

Rust 2018

## Usage

Running `cargo build` at the top-level of this directory produces a functional executable `target/debug/two_phase_commit`, which accepts the following command-line parameters

- `-l`: specifies the path to the directory where logs are stored. Default is `tmp/`.
- `-m`: mode. "run" starts 2PC, "client" starts a client process, "participant" starts a participant process, "check" checks logs produced by the previous run.
- `-c`: number of clients making requests.
- `-p`: number of participants in protocol
- `-r`: number of requests made per client
- `-s`: probability participants successfully execute requests
- `-S`: probability participants successfully send messages
- `-v`: output verbosity. 0 -> no output, 5 -> output everything

When the project is run in "run" mode, it starts the coordinator and launches the client and participant processes and stores the commit logs in `./tmp`. Clients and participants are launched by spawning new children processes with the same arguments, except in the "client" and "participant" mode respectively. When the project is run in "check" mode, it will analyze the commit logs produced by the last run to determine whether the 2PC protocol handled the specified number of client requests correctly.