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
    match s.to_uppercase().trim() {
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
  len: usize,
  arg_idx: usize,
  arguments: [String; MAX_ARGS],
  pub name: Option<InstructionName>,
  config: Arc<Mutex<Storage>>,
  storage: Arc<Mutex<Storage>>,
  tx: mpsc::Sender<ExpiryMsg>,
}

impl Instruction {
  pub fn new(
    config: Arc<Mutex<Storage>>,
    storage: Arc<Mutex<Storage>>,
    tx: mpsc::Sender<ExpiryMsg>,
  ) -> Self {
    Self {
      len: 0,
      arg_idx: 0,
      arguments: std::array::from_fn(|_| String::new()),
      name: None,
      config,
      storage,
      tx,
    }
  }

  pub fn clear(&mut self) {
    self.len = 0;
    self.arg_idx = 0;
    self.arguments = std::array::from_fn(|_| String::new());
    self.name = None;
  }

  pub fn is_ready(&self) -> bool {
    let mut nonempty = self.arguments.iter().take(self.len - 1);
    self.name.is_some() && nonempty.all(|s| !s.is_empty())
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
        if self.arguments[2] == *"px" {
          let duration_ms = self.arguments[3]
            .trim()
            .parse::<u64>()
            .expect("Invalid duration");
          self
            .tx
            .send((self.arguments[0].clone(), duration_ms))
            .unwrap();
        }

        b"+OK\r\n".to_vec()
      }
      InstructionName::Config => {
        let conf = self.config.lock().unwrap();
        let filtered: HashMap<String, String> = conf
          .iter()
          .filter(|(key, _)| **key == self.arguments[1])
          .map(|(k, v)| (k.clone(), v.clone()))
          .collect();

        Self::serialize_resp(&filtered).into_bytes()
      }
    }
  }

  pub fn parse_args_length(&mut self, instr: &str) -> &mut Self {
    let number_str = &instr[1..instr.len()]; // skip '*' and trailing ""
    self.len = number_str.parse::<usize>().unwrap();
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

  fn serialize_resp(map: &HashMap<String, String>) -> String {
    let mut out = String::new();
    out.push_str(&format!("*{}\r\n", map.len() * 2));

    for (key, value) in map {
      out.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
      out.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
    }

    out
  }
}
