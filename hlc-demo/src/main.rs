#[macro_use]
extern crate futures;
extern crate failure;
extern crate hybrid_clocks;
extern crate structopt;
extern crate tokio;
#[macro_use]
extern crate log;
extern crate env_logger;

use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use failure::Error;
use futures::{Future, Stream};
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
    to_send: Option<(Vec<u8>, SocketAddr)>,
    socket: UdpSocket,
}

impl Future for Listener {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        unimplemented!()
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
                let _ = try_ready!(self.socket.poll_send_to(&msg, &peer));
                info!("Sent {} bytes to {}", msg.len(), peer);
                self.to_send = None;
            }

            match try_ready!(self.notifications.poll()) {
                None => {
                    info!("Stream ended");
                    return Ok(Async::Ready(()));
                }
                Some(n) => {
                    info!("Notification: {:?}", n);
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

    let _listener = Listener { socket };

    let socket = UdpSocket::bind(&"0.0.0.0:0".parse().expect("parse 0"))?;
    let notifications = tokio::timer::Interval::new(Instant::now(), Duration::from_secs(1));
    let to_send = None;
    info!("Client socket on: {}", socket.local_addr()?);
    let client = Client {
        notifications,
        socket,
        to_send,
    };

    tokio::run(client.map_err(|e| println!("Listener error = {:?}", e)));
    Ok(())
}
