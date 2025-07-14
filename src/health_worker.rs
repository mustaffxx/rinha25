use std::thread;
use std::sync::mpsc::{self, Sender, Receiver};
use std::time::Duration;


pub struct HealthWorker {
    sender: Sender<HealthMessage>,
}

#[derive(Debug)]
pub enum HealthMessage {
    Check,
    Shutdown,
}

impl HealthWorker {
    pub fn start() -> Self {
        let (tx, rx): (Sender<HealthMessage>, Receiver<HealthMessage>) = mpsc::channel();

        thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok(HealthMessage::Check) => {
                        // Perform health check logic here
                        println!("Health check performed.");
                    }
                    Ok(HealthMessage::Shutdown) => {
                        println!("Health worker shutting down.");
                        break;
                    }
                    Err(_) => break,
                }
                thread::sleep(Duration::from_secs(1));
            }
        });

        HealthWorker { sender: tx }
    }

    pub fn check(&self) {
        let _ = self.sender.send(HealthMessage::Check);
    }

    pub fn shutdown(&self) {
        let _ = self.sender.send(HealthMessage::Shutdown);
    }
}