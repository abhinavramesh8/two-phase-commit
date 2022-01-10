//! 
//! coordinator.rs
//! Implementation of 2PC coordinator
//! 
use std::thread;
use std::sync::{Arc};
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use crate::message;
use message::ProtocolMessage;
use message::MessageType;
use crate::oplog;
use log::*;

/// Coordinator
/// struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    log: oplog::OpLog,
    from_client_rx: Receiver<ProtocolMessage>,
    client_tx: Sender<ProtocolMessage>,
    to_client_tx_vec: Vec<Sender<ProtocolMessage> >,
    from_participant_rx: Receiver<ProtocolMessage>,
    participant_tx: Sender<ProtocolMessage>,
    to_participant_tx_vec: Vec<Sender<ProtocolMessage> >
}

///
/// Coordinator
/// implementation of coordinator functionality
/// Required:
/// 1. new -- ctor
/// 2. protocol -- implementation of coordinator side of protocol
/// 3. report_status -- report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- what to do when a participant joins
/// 5. client_join -- what to do when a client joins
/// 
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    /// 
    /// <params>
    ///     logpath: directory for log files --> create a new log there. 
    ///     r: atomic bool --> still running?
    ///     success_prob --> probability operations/sends succeed
    ///
    pub fn new(
        logpath: String) -> Coordinator {
        let (client_tx, from_client_rx) = mpsc::channel();
        let (participant_tx, from_participant_rx) = mpsc::channel();
        Coordinator {
            log: oplog::OpLog::new(logpath),
            from_client_rx,
            client_tx,
            to_client_tx_vec: Vec::new(),
            from_participant_rx,
            participant_tx,
            to_participant_tx_vec: Vec::new()
        }
    }

    /// 
    /// participant_join()
    /// handle the addition of a new participant
    /// HINT: keep track of any channels involved!
    /// HINT: you'll probably need to change this routine's 
    ///       signature to return something!
    ///       (e.g. channel(s) to be used)
    /// 
    pub fn participant_join(&mut self) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {

        // assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        let participant_tx = mpsc::Sender::clone(&(self.participant_tx));
        let (to_participant_tx, participant_rx) = mpsc::channel();
        self.to_participant_tx_vec.push(to_participant_tx);   
        return (participant_tx, participant_rx);
    }

    /// 
    /// client_join()
    /// handle the addition of a new client
    /// HINTS: keep track of any channels involved!
    /// HINT: you'll probably need to change this routine's 
    ///       signature to return something!
    ///       (e.g. channel(s) to be used)
    /// 
    pub fn client_join(&mut self) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {

        // assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        let client_tx = mpsc::Sender::clone(&(self.client_tx));
        let (to_client_tx, client_rx) = mpsc::channel();
        self.to_client_tx_vec.push(to_client_tx);   
        return (client_tx, client_rx);
    }

    /// 
    /// send()
    /// send a message, maybe drop it
    /// HINT: you'll need to do something to implement 
    ///       the actual sending!
    /// 
    pub fn send(sender: &Sender<ProtocolMessage>, pm: ProtocolMessage) {
        sender.send(pm).expect("Error sending from coordinator!");
    }     

    /// 
    /// recv_request()
    /// receive a message from a client
    /// to start off the protocol.
    /// 
    pub fn recv_request(&self) -> ProtocolMessage {
        // assert!(self.state == CoordinatorState::Quiescent);        
        trace!("coordinator::recv_request...");

        // TODO: write me!
        let pm = self.from_client_rx.recv().expect("Error receiving from client!");

        trace!("leaving coordinator::recv_request");
        pm
    }        

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    /// 
    pub fn protocol(mut self, n_transactions: i32, running: Arc<AtomicBool>) {
        // TODO!
        let n_participants = self.to_participant_tx_vec.len() as i32;
        let mut successful_ops = 0;
        let mut failed_ops = 0;
        for _ in 0..n_transactions {
            if !running.load(Ordering::SeqCst) {
                break;
            }
            let pm = self.recv_request();
            self.log.append(&pm);
            
            for to_participant_tx in self.to_participant_tx_vec.iter() {
                let message_type = MessageType::CoordinatorPropose;
                let pm = ProtocolMessage::generate(message_type, pm.txid, 0);
                self.log.append(&pm);
                Self::send(to_participant_tx, pm);
            }
            
            thread::sleep(Duration::from_millis(50));
            let mut vote_commit = true;
            let mut n_votes = 0;
            while let Ok(pm) = self.from_participant_rx.try_recv() {
                if let MessageType::ParticipantVoteAbort = pm.mtype {
                    vote_commit = false;
                }
                self.log.append(&pm);
                n_votes += 1;
            }
            if n_votes < n_participants {
                vote_commit = false;
            }
            
            let (participant_message_type, client_message_type) = if vote_commit {
                successful_ops += 1;
                (MessageType::CoordinatorCommit, MessageType::ClientResultCommit)
            } else {
                failed_ops += 1;
                (MessageType::CoordinatorAbort, MessageType::ClientResultAbort)
            };
            
            for (i, to_participant_tx) in self.to_participant_tx_vec.iter().enumerate() {
                let pm = ProtocolMessage::generate(participant_message_type, 
                                                   pm.txid, 0);
                if i == 0 {
                    self.log.append(&pm);
                }
                Self::send(to_participant_tx, pm);
            }
            
            let client_id = pm.sender_id;
            let pm = ProtocolMessage::generate(client_message_type, pm.txid, 0);
            self.log.append(&pm);
            Self::send(&self.to_client_tx_vec[client_id as usize], pm);
        }
        
        let message_type = MessageType::CoordinatorExit;
        for (i, to_participant_tx) in self.to_participant_tx_vec.iter().enumerate() {
            let pm = ProtocolMessage::generate(message_type, -1, 0);
            if i == 0 {
                self.log.append(&pm);
            }
            Self::send(to_participant_tx, pm);
        }
        
        let mut unknown_ops = 0;
        while let Ok(_) = self.from_client_rx.try_recv() {
            unknown_ops += 1;
        }
        
        for to_client_tx in self.to_client_tx_vec.iter() {
            let pm = ProtocolMessage::generate(message_type, -1, 0);
            Self::send(to_client_tx, pm);
        }
        
        println!("coordinator:\tC:{}\tA:{}\tU:{}", successful_ops, failed_ops, unknown_ops);                    
    }
}
