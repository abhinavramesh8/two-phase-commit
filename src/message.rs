//! 
//! message.rs
//! Message type/primitives
//! 
//! 
//! YOU SHOULD NOT NEED TO CHANGE CODE IN THIS FILE.
//! 
use std::sync::atomic::{AtomicI32, Ordering};
use serde_json::Value;

///
/// MessageType
/// Message type codes that various 2PC parties may want to send 
/// or receive. 
/// 
/// HINT: You should find it necessary to add to this list!
/// 
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageType {    
    ClientRequest,          // Request a transaction from the coordinator
    CoordinatorPropose,     // Coordinator sends propose work to participants
    ParticipantVoteCommit,  // Participant votes to commit in phase 1
    ParticipantVoteAbort,   // Participant votes to abort in phase 1
    CoordinatorAbort,       // Coordinator aborts in phase 2
    CoordinatorCommit,      // Coordinator commits phase 2
    ClientResultCommit,     // result (success/fail) communicated to client
    ClientResultAbort,      // result (success/fail) communicated to client
    CoordinatorExit,        // Coordinator telling client/participant about shut down
}

/// generator for unique ids of messages
static COUNTER: AtomicI32 = AtomicI32::new(1);

///
/// ProtocolMessage
/// message struct to be send as part of 2PC protocol
/// HINT: you probably don't need to change this one. 
///       you can certainly add if it helps, though.
/// 
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ProtocolMessage {
    pub mtype: MessageType,
    pub uid: i32,
    pub txid: i32,
    pub sender_id: i32
}

///
/// ProtocolMessage implementation
/// 
impl ProtocolMessage {
    pub fn generate(t: MessageType, tid: i32, sid: i32) -> ProtocolMessage {
        ProtocolMessage {
            mtype: t,
            uid: COUNTER.fetch_add(1, Ordering::SeqCst),
            txid: tid,
            sender_id: sid
        }
    }

    pub fn from_string(line: &String) -> ProtocolMessage {
        let data: Value = serde_json::from_str(&line.to_string()).unwrap();
        let pm: ProtocolMessage = serde_json::from_value(data).unwrap();
        pm
    }

}
