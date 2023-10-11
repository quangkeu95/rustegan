use derive_more::Deref;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Message {
    pub src: NodeId,
    pub dest: NodeId,
    pub body: MessageBody,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeId(String);

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MessageBody {
    pub msg_id: Option<MessageId>,
    pub in_reply_to: Option<MessageId>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

pub type MessageId = usize;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_deserialize_message() {
        let message_json = r#"
            {
              "src": "c1",
              "dest": "n1",
              "body": {
                "type": "echo",
                "msg_id": 1,
                "echo": "Please echo 35"
              }
            }
        "#;

        let message: Message = serde_json::from_str(message_json).unwrap();
        assert_eq!(message.src.0, "c1".to_string());
        assert_eq!(message.dest.0, "n1".to_string());
        assert_eq!(message.body.msg_id.unwrap(), 1);
        match message.body.payload {
            Payload::Echo { echo } => {
                assert_eq!(echo.as_str(), "Please echo 35");
            }
            _ => panic!("unexpected payload"),
        }
    }
}
