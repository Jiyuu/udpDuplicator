#[macro_use]
extern crate clap;
extern crate crc32fast;

use crc32fast::Hasher;
use clap::{Arg, App, SubCommand, ArgGroup};
use std::net::{UdpSocket, SocketAddr, ToSocketAddrs};
use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::collections::HashSet;

fn main() {
    let matches = App::new("udpDuplicator")
        .version(crate_version!())
        .author(crate_authors!())
        .about("duplicates udp packets to target, attampting to ensure timely delivery")
        .arg(Arg::with_name("port")
            .help("the target of this relay socket")
            .value_name("PORT")
            .required(true))
        .arg(Arg::with_name("target")
            .help("the target of this relay socket")
            .value_name("URL:PORT")
            .required(true))
        .get_matches();


    let target = matches.value_of("target").unwrap().to_socket_addrs().unwrap().next().unwrap();
    let port: u16 = value_t_or_exit!(matches,"port",u16);

    duplicate_packets(target, port)
}

fn duplicate_packets(target: SocketAddr, port: u16) {
    let socket = Arc::new(UdpSocket::bind(SocketAddr::new("0.0.0.0".parse().unwrap(), port)).unwrap());
    socket.set_nonblocking(true);
    socket.set_read_timeout(None);

    let target_socket = Arc::new(UdpSocket::bind("0.0.0.0:44212").unwrap());
    target_socket.connect(target).unwrap();
    target_socket.set_nonblocking(true);


    let mut data: [u8; 65535] = [0; 65535];
    let (mut length, source_address) = socket.recv_from(&mut data).unwrap();
    socket.connect(source_address).unwrap();
    socket.set_nonblocking(true);


    let handler = thread::spawn({
        let source = target_socket.try_clone().unwrap();
        let target = socket.try_clone().unwrap();
        move || {
            deduplicate_packets(source, target);
        }
    }
    );

    let mut repeat_count = 0;

    loop {
        if repeat_count < 1000 {
            target_socket.send(&data[..length]).unwrap();
            repeat_count = repeat_count + 1;
        }
        if let Ok(length) = socket.recv(&mut data) {
            repeat_count = 0;
        } else {
            thread::sleep(Duration::from_millis(1));
            if let Ok(length) = socket.recv(&mut data) {
                repeat_count = 0;
            }
        }
    }
    handler.join().unwrap();
}

fn deduplicate_packets(source_socket: UdpSocket, target_socket: UdpSocket) {
    let mut data: [u8; 65535] = [0; 65535];
    let mut sent_packets = HashSet::new();
    loop {
        let length = source_socket.recv(&mut data).unwrap();
        let mut hasher = Hasher::new();
        hasher.update(&data[..length]);
        let hash = hasher.finalize();

        if !sent_packets.contains(&hash) {
            target_socket.send(&data[..length]).unwrap();
            sent_packets.insert(hash);
        }
    }
}