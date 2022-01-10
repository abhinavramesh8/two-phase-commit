//! 
//! participant.rs
//! Implementation of 2PC participant
//! 
use std::sync::mpsc::{Sender, Receiver};
use crate::message::MessageType;
use crate::message::ProtocolMessage;
use crate::oplog;
use log::*;

///
/// Participant
/// structure for maintaining per-participant state 
/// and communication/synchronization objects to/from coordinator
/// 
#[derive(Debug)]
pub struct Participant {    
    id: i32,
    log: oplog::OpLog,
    op_success_prob: f64,
    msg_success_prob: f64,
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>
}

///
/// Participant
/// implementation of per-participant 2PC protocol
/// Required:
/// 1. new -- ctor
/// 2. pub fn report_status -- reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- implements participant side protocol
///
impl Participant {

    /// 
    /// new()
    /// 
    /// Return a new participant, ready to run the 2PC protocol
    /// with the coordinator. 
    /// 
    /// HINT: you may want to pass some channels or other communication 
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this ctor.
    /// HINT: you may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course. 
    /// 
    pub fn new(
        i: i32, 
        tx: Sender<ProtocolMessage>, 
        rx: Receiver<ProtocolMessage>, 
        logpath: String,
        f_success_prob_ops: f64,
        f_success_prob_msg: f64) -> Participant {

        Participant {
            id: i,
            log: oplog::OpLog::new(logpath),
            op_success_prob: f_success_prob_ops,
            msg_success_prob: f_success_prob_msg,
            tx,
            rx
        }   
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator.
    /// This variant can be assumed to always succeed.
    /// You should make sure your solution works using this 
    /// variant before working with the send_unreliable variant.
    /// 
    /// HINT: you will need to implement something that does the 
    ///       actual sending.
    /// 
    pub fn send(&self, pm: ProtocolMessage) {
        // TODO
        self.tx.send(pm).expect("Error sending from participant!");
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator, 
    /// with some probability of success thresholded by the 
    /// command line option success_probability [0.0..1.0].
    /// This variant can be assumed to always succeed
    /// 
    /// HINT: you will need to implement something that does the 
    ///       actual sending, but you can use the threshold 
    ///       logic in this implementation below. 
    /// 
    pub fn send_unreliable(&self, pm: ProtocolMessage) {
        let x: f64 = rand::random();
        if x < self.msg_success_prob {
            self.send(pm);
        }
    }    

    /// 
    /// perform_operation
    /// perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the 
    /// command-line option success_probability. 
    /// 
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic. 
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than 
    ///       bool if it's more convenient for your design).
    /// 
    pub fn perform_operation(&self) -> bool {

        trace!("participant::perform_operation");
        let x: f64 = rand::random();
        let result: bool = x <= self.op_success_prob;

        trace!("exit participant::perform_operation");
        result
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    /// 
    pub fn protocol(mut self) {
        
        trace!("Participant_{}::protocol", self.id);

        // TODO
        let mut successful_ops = 0;
        let mut failed_ops = 0;
        loop {
            let pm = self.rx.recv().expect("Error receiving from coordinator for participant!");
            self.log.append(&pm);
            match pm.mtype {
                MessageType::CoordinatorExit => break,
                MessageType::CoordinatorCommit => successful_ops += 1,
                MessageType::CoordinatorAbort => failed_ops += 1,
                MessageType::CoordinatorPropose => {
                    let message_type = if self.perform_operation() {
                        MessageType::ParticipantVoteCommit
                    } else {
                        MessageType::ParticipantVoteAbort
                    };
                    let pm = ProtocolMessage::generate(message_type, pm.txid, self.id);
                    self.log.append(&pm);
                    self.send_unreliable(pm);
                }
                _ => { }
            }
        }
        println!("participant_{}:\tC:{}\tA:{}\tU:0", self.id, successful_ops, failed_ops);
        // self.report_status();
    }
}
