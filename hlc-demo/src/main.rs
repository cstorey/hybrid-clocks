#[macro_use]
extern crate futures;
extern crate failure;
extern crate hybrid_clocks;
extern crate structopt;
extern crate tokio;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate rand;
extern crate serde_json;

use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use failure::Error;
use futures::{Future, Stream};
use hybrid_clocks::{Clock, Timestamp, Wall, WallT};
use rand::Rng;
use structopt::StructOpt;
use tokio::net::UdpSocket;
use tokio::prelude::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "hlc-demo", about = "Hybrid Logical clocks demo")]
struct Opt {
    #[structopt(short = "l", long = "listener", help = "listening socket address")]
    listen_addr: SocketAddr,
    #[structopt(help = "peers")]
    peers: Vec<SocketAddr>,
}

struct Listener {
    socket: UdpSocket,
}

struct Client<S> {
    notifications: S,
    to_send: Option<(Timestamp<WallT>, SocketAddr)>,
    socket: UdpSocket,
    clock: Clock<Wall>,
    peers: Vec<SocketAddr>,
}

impl Future for Listener {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        let mut buf = [0; 1024];
        let (recvd, peer) = try_ready!(self.socket.poll_recv_from(&mut buf));
        debug!("Received {:?} bytes from {}", recvd, peer);
        let d: Timestamp<WallT> = serde_json::from_slice(&buf[0..recvd])?;
        info!("Update from {}: {}", peer, d);
        Ok(Async::NotReady)
    }
}

impl<
        S: Stream<Item = I, Error = E>,
        I: fmt::Debug,
        E: 'static + Send + Sync + std::error::Error,
    > Future for Client<S>
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        loop {
            if let Some((msg, peer)) = self.to_send.as_ref() {
                let bs = serde_json::to_vec(&msg)?;
                let slen = try_ready!(self.socket.poll_send_to(&bs, &peer));
                info!("Sent {} ({} bytes) to {}", msg, slen, peer);
                self.to_send = None;
            }

            match try_ready!(self.notifications.poll()) {
                None => {
                    info!("Stream ended");
                    return Ok(Async::Ready(()));
                }
                Some(n) => {
                    info!("Notification: {:?}", n);
                    let idx = rand::thread_rng().gen_range(0, self.peers.len());
                    let peer = self.peers[idx].clone();
                    let t = self.clock.now();
                    debug!("Queueing to:{}; msg:{}", peer, t);
                    self.to_send = Some((t, peer))
                }
            }
        }
    }
}

fn main() -> Result<(), Error> {
    env_logger::init();

    let opt = Opt::from_args();
    debug!("{:?}", opt);

    let socket = UdpSocket::bind(&opt.listen_addr)?;
    info!("Listening on: {}", socket.local_addr()?);

    let listener = Listener { socket };

    let socket = UdpSocket::bind(&"0.0.0.0:0".parse().expect("parse 0"))?;
    let notifications = tokio::timer::Interval::new(Instant::now(), Duration::from_secs(1));
    let to_send = None;
    info!("Client socket on: {}", socket.local_addr()?);
    let clock = Clock::wall();
    let client = Client {
        notifications,
        socket,
        to_send,
        clock,
        peers: opt.peers,
    };

    tokio::run(
        client
            .select(listener)
            .map(|_| ())
            .map_err(|(e, _)| e)
            .map_err(|e| println!("Listener error = {:?}", e)),
    );
    Ok(())
}
