extern crate failure;
extern crate futures;
extern crate hybrid_clocks;
extern crate structopt;
extern crate tokio;
#[macro_use]
extern crate log;
extern crate bytes;
extern crate env_logger;
extern crate rand;
extern crate rmp_serde;
extern crate serde;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use failure::Error;
use futures::prelude::*;
use hybrid_clocks::{Clock, Timestamp, Wall2T};
use rand::Rng;
use structopt::StructOpt;
use tokio::net::{UdpFramed, UdpSocket};

#[derive(Debug, StructOpt)]
#[structopt(name = "hlc-demo", about = "Hybrid Logical clocks demo")]
struct Opt {
    #[structopt(short = "l", long = "listener", help = "listening socket address")]
    listen_addr: SocketAddr,
    #[structopt(help = "peers")]
    peers: Vec<SocketAddr>,
}

#[derive(Copy, Clone, Debug)]
struct MsgpackCodec<T>(std::marker::PhantomData<T>);

fn main() -> Result<(), Error> {
    env_logger::Builder::from_default_env()
        .default_format_timestamp_nanos(true)
        .init();

    let opt = Opt::from_args();
    debug!("{:?}", opt);

    let socket = UdpSocket::bind(&opt.listen_addr)?;
    info!("Listening on: {}", socket.local_addr()?);
    let (client, listener) = UdpFramed::new(
        socket,
        MsgpackCodec::<Timestamp<Wall2T>>(std::marker::PhantomData),
    )
    .split();
    let clock = Arc::new(Mutex::new(Clock::wall2()));

    let listener = {
        let clock = clock.clone();
        listener.map(move |(observation, peer)| {
            info!("Update from {}: {}", peer, observation);
            let now = clock.lock().expect("lock clock").now();
            let cdelta = observation.time - now.time;
            let glt = observation.time.cmp(&now.time);
            let counter = observation.count;
            use std::cmp::Ordering;
            debug!(
                "Recieved clock (msg:{} {} local:{})",
                observation,
                match glt {
                    Ordering::Greater => '>',
                    Ordering::Equal => '=',
                    Ordering::Less => '<',
                },
                now,
            );
            info!("Recieved clock delta:{}; counter:{}", cdelta, counter);
            clock
                .lock()
                .expect("lock clock")
                .observe(&observation)
                .expect("observe");
            clock.lock().expect("lock clock").now()
        })
    };

    let notifications = {
        tokio::timer::Interval::new(Instant::now(), Duration::from_secs(1))
            .inspect(|_| info!("Interval tick"))
            .map(move |_| clock.lock().expect("lock clock").now())
            .map_err(Into::into)
    };

    let pick_random_peer = {
        let peers = opt.peers.clone();
        move |t| {
            let idx = rand::thread_rng().gen_range(0, peers.len());
            let peer = peers[idx].clone();
            debug!("Queueing to:{}; msg:{}", peer, t);
            (t, peer)
        }
    };

    let root_task = listener
        .select(notifications)
        .map(pick_random_peer)
        .forward(client)
        .and_then(|(_src, mut sink)| {
            debug!("Closing client");
            futures::future::poll_fn(move || {
                trace!("Try close client");
                sink.close()
            })
        });
    tokio::run(root_task.map_err(|e| println!("Listener error = {:?}", e)));
    Ok(())
}

impl<T: serde::de::DeserializeOwned> tokio::codec::Decoder for MsgpackCodec<T> {
    type Item = T;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let val = rmp_serde::from_slice(&*src)?;
        Ok(Some(val))
    }
}

impl<T: serde::Serialize> tokio::codec::Encoder for MsgpackCodec<T> {
    type Item = T;
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut ser = rmp_serde::Serializer::new_named(BufWr(dst));
        item.serialize(&mut ser)?;
        Ok(())
    }
}

struct BufWr<'a>(&'a mut BytesMut);
impl<'a> std::io::Write for BufWr<'a> {
    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
    fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        self.0.extend_from_slice(data);
        Ok(data.len())
    }
}
