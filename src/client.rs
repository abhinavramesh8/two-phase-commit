//! 
//! client.rs
//! Implementation of 2PC client
//! 
use std::sync::mpsc::{Sender, Receiver};
use std::sync::atomic::{AtomicI32, Ordering};
use crate::message::{MessageType, ProtocolMessage};
use log::*;

// static counter for getting unique TXID numbers
static TXID_COUNTER: AtomicI32 = AtomicI32::new(1);

// client state and 
// primitives for communicating with 
// the coordinator
#[derive(Debug)]
pub struct Client {    
    pub id: i32,
    //running: &Arc<AtomicBool>,
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,
    n_requests: i32
}

///
/// client implementation
/// Required: 
/// 1. new -- ctor
/// 2. pub fn report_status -- reports number of committed/aborted/unknown 
/// 3. pub fn protocol(&mut self, n_requests: i32) -- implements client side protocol
///
impl Client {

    /// 
    /// new()
    /// 
    /// Return a new client, ready to run the 2PC protocol
    /// with the coordinator. 
    /// 
    /// HINT: you may want to pass some channels or other communication 
    ///       objects that enable coordinator->client and client->coordinator
    ///       messaging to this ctor.
    /// HINT: you may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor
    /// 
    pub fn new(i: i32,
               n_requests: i32,
               tx: Sender<ProtocolMessage>,
               rx: Receiver<ProtocolMessage>) -> Client {
        Client {
            id: i,
            tx,
            rx,
            n_requests
        }   
    }

    /// 
    /// send_next_operation(&mut self)
    /// send the next operation to the coordinator
    /// 
    pub fn send_next_operation(&self, request_no: i32) {

        trace!("Client_{}::send_next_operation", self.id);

        // create a new request with a unique TXID.         
        // let request_no: i32 = 0; // TODO--choose another number!
        let transaction_id = TXID_COUNTER.fetch_add(1, Ordering::SeqCst);

        info!("Client {} request({})->txid:{} called", self.id, request_no, transaction_id);
        let pm = ProtocolMessage::generate(MessageType::ClientRequest, 
                                           transaction_id, self.id);

        info!("client {} calling send...", self.id);

        // TODO
        self.tx.send(pm).expect("Error sending from client to coordinator!");

        trace!("Client_{}::exit send_next_operation", self.id);
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the 
    /// last issued request. Note that we assume the coordinator does 
    /// not fail in this simulation
    ///
    pub fn recv_result(&self) -> ProtocolMessage {
        self.rx.recv().expect("Error receiving result from coordinator!")
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this client before exiting. 
    /// 
    pub fn report_status(&self, successful_ops: i32, failed_ops: i32, unknown_ops: i32) {
        // TODO: collect real stats!
        println!("client_{}:\tC:{}\tA:{}\tU:{}", self.id, successful_ops, 
                 failed_ops, unknown_ops);
    }    

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    /// 
    pub fn protocol(self) {

        // run the 2PC protocol for each of n_requests

        // TODO
        let mut successful_ops = 0;
        let mut failed_ops = 0;
        for request_no in 0..(self.n_requests) {
            self.send_next_operation(request_no);
            let pm = self.recv_result();
            match pm.mtype {
                MessageType::ClientResultCommit => successful_ops += 1,
                MessageType::ClientResultAbort => failed_ops += 1,
                MessageType::CoordinatorExit => {
                    self.report_status(successful_ops, failed_ops, 1);
                    return;
                },
                _ => { }
            }
        }
        self.recv_result();
        self.report_status(successful_ops, failed_ops, 0);
    }
}
