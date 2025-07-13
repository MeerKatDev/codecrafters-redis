use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{mpsc, Arc, Mutex};

pub type Storage = HashMap<String, String>;
pub type ExpiryMsg = (String, u64);

const MAX_ARGS: usize = 5;

pub enum InstructionName {
  Ping,
  Echo,
  Get,
  Set,
  Config,
}

impl FromStr for InstructionName {
  type Err = ();

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s.trim() {
      "PING" => Ok(InstructionName::Ping),
      "ECHO" => Ok(InstructionName::Echo),
      "GET" => Ok(InstructionName::Get),
      "SET" => Ok(InstructionName::Set),
      "CONFIG" => Ok(InstructionName::Config),
      _ => Err(()),
    }
  }
}

pub struct Instruction {
  len: u8,
  arg_idx: usize,
  arguments: [String; MAX_ARGS],
  pub name: Option<InstructionName>,
  storage: Arc<Mutex<Storage>>,
  tx: mpsc::Sender<ExpiryMsg>,
}

impl Instruction {
  pub fn new(storage: Arc<Mutex<Storage>>, tx: mpsc::Sender<ExpiryMsg>) -> Self {
    Self {
      len: 0,
      arg_idx: 0,
      arguments: std::array::from_fn(|_| String::new()),
      name: None,
      storage,
      tx,
    }
  }

  pub fn make_response(&mut self) -> Vec<u8> {
    match self.name.as_ref().expect("Should have been filled!") {
      InstructionName::Ping => b"+PONG\r\n".to_vec(),
      InstructionName::Echo => format!("+{}\r\n", self.arguments[0]).into_bytes(),
      InstructionName::Get => {
        let storage = self.storage.lock().unwrap();
        match storage.get(&self.arguments[0]) {
          Some(val) => format!("+{}\r\n", val).into_bytes(),
          None => b"$-1\r\n".to_vec(),
        }
      }
      InstructionName::Set => {
        let mut storage = self.storage.lock().unwrap();
        storage.insert(self.arguments[0].to_string(), self.arguments[1].to_string());
        if self.arguments[2] == *"px\r\n" {
          let duration_ms = self.arguments[3]
            .trim()
            .parse::<u64>()
            .expect("Invalid duration");
          self
            .tx
            .send((self.arguments[1].clone(), duration_ms))
            .unwrap();
        }

        b"+OK\r\n".to_vec()
      }
      InstructionName::Config => b"+OK\r\n".to_vec(),
    }
  }

  pub fn parse_args_length(&mut self, instr: &str) -> &mut Self {
    let number_str = &instr[1..instr.len() - 2]; // skip '*' and trailing "\r\n"
    self.len = number_str.parse::<u8>().unwrap();
    self
  }

  pub fn parse_command(&mut self, instr: &str) -> &mut Self {
    if let Ok(name) = InstructionName::from_str(instr.trim()) {
      self.name = Some(name);
    }
    self
  }

  pub fn parse_argument(&mut self, instr: &str) -> &mut Self {
    self.arguments[self.arg_idx] = instr.to_string();
    self.arg_idx += 1;
    self
  }
}
