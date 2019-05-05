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

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use failure::Error;
use futures::prelude::*;
use futures::{Future, Sink, Stream};
use hybrid_clocks::{Clock, Timestamp, WallT};
use rand::Rng;
use structopt::StructOpt;
use tokio::net::UdpSocket;

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

struct Client {
    to_send: Option<(Timestamp<WallT>, SocketAddr)>,
    socket: UdpSocket,
    peers: Vec<SocketAddr>,
}

impl Stream for Listener {
    type Item = Timestamp<WallT>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Timestamp<WallT>>, Error> {
        let mut buf = [0; 1024];
        let (recvd, peer) = try_ready!(self.socket.poll_recv_from(&mut buf));
        debug!("Received {:?} bytes from {}", recvd, peer);
        let d: Timestamp<WallT> = serde_json::from_slice(&buf[0..recvd])?;
        info!("Update from {}: {}", peer, d);
        Ok(Async::Ready(Some(d)))
    }
}

impl Sink for Client {
    type SinkItem = Timestamp<WallT>;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if self.to_send.is_some() {
            return Ok(AsyncSink::NotReady(item));
        }

        let idx = rand::thread_rng().gen_range(0, self.peers.len());
        let peer = self.peers[idx].clone();
        debug!("Queueing to:{}; msg:{}", peer, item);
        self.to_send = Some((item, peer));
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if let Some((msg, peer)) = self.to_send.as_ref() {
            let bs = serde_json::to_vec(&msg)?;
            let slen = try_ready!(self.socket.poll_send_to(&bs, &peer));
            info!("Sent {} ({} bytes) to {}", msg, slen, peer);
            self.to_send = None;
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
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
    let notifications = {
        let mut clock = Clock::wall();
        tokio::timer::Interval::new(Instant::now(), Duration::from_secs(1))
            .map(move |_| clock.now())
            .map_err(Into::into)
    };

    let to_send = None;
    info!("Client socket on: {}", socket.local_addr()?);
    let client = Client {
        socket,
        to_send,
        peers: opt.peers,
    };

    tokio::run(
        listener
            .select(notifications)
            .forward(client)
            .map(|(_src, _sink)| ())
            .map_err(|e| println!("Listener error = {:?}", e)),
    );
    Ok(())
}
