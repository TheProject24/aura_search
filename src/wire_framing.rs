use std::io::{Error, ErrorKind::InvalidData, Read, Write};

const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;

pub struct FramedNetworkStream<S> {
    stream: S,
}

impl<S: Read + Write> FramedNetworkStream<S> {
    pub fn new(stream: S) -> Self {
        FramedNetworkStream { stream }
    }

    pub fn send_frame(&mut self, payload: &[u8]) -> Result<(), Error> {
        let length = payload.len() as u32;

        if length > MAX_PAYLOAD_SIZE {
            return Err(Error::new(InvalidData, "Payload too massive to send!"));
        }

        let length_header = length.to_be_bytes();
        self.stream.write_all(&length_header)?;
        self.stream.write_all(payload)?;
        self.stream.flush()?;

        Ok(())
    }

    pub fn receive_frame(&mut self) -> Result<Vec<u8>, Error> {
        let mut length_header = [0u8; 4];
        self.stream.read_exact(&mut length_header)?;
        let length = u32::from_be_bytes(length_header);

        if length > MAX_PAYLOAD_SIZE {
            return Err(Error::new(InvalidData, "Payload too large"));
        }

        let mut payload = vec![0u8; length as usize];
        self.stream.read_exact(&mut payload)?;
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_sticky_note_framing() {
        let mock_network_pipe = Cursor::new(Vec::new());
        let mut network = FramedNetworkStream::new(mock_network_pipe);

        let message_one = b"HELLO";
        let message_two = b"CAT";

        network.send_frame(message_one).unwrap();
        network.send_frame(message_two).unwrap();

        network.stream.set_position(0);
        let received_one = network.receive_frame().unwrap();
        let received_two = network.receive_frame().unwrap();

        assert_eq!(received_one, b"HELLO");
        assert_eq!(received_two, b"CAT");

        println!("Successfully sent and received framed packets without mixing!");
    }
}
