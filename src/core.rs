use std::io::BufRead;

use crate::{
    message::{Event, Message, MessageBody},
    node::Init,
};

use super::node::Node;
use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

pub fn main_loop<S, N, P, C>(init_state: S) -> anyhow::Result<()>
where
    N: Node<S, P, C>,
    P: DeserializeOwned + Send + 'static,
    C: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();

    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    // read init message
    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("no init message received")
            .context("cannot read init msg from STDIN")?,
    )
    .context("cannot deserialize init message")?;

    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("first message should be init message");
    };

    let mut node: N = Node::from_init(init_state, init, tx.clone())?;
    let reply = Message {
        src: init_msg.dest,
        dest: init_msg.src,
        body: MessageBody {
            msg_id: Some(0),
            in_reply_to: init_msg.body.msg_id,
            payload: InitPayload::InitOk,
        },
    };

    reply.send(&mut stdout).context("send response to init")?;

    drop(stdin);
    let handle = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for line in stdin.lines() {
            let input = line.context("Maelstrom input from STDIN could not be read")?;
            let msg: Message<P> = serde_json::from_str(&input)
                .context("Maelstrom input from STDIN could not be deserialized")?;

            if let Err(_) = tx.send(Event::Message(msg)) {
                return Ok::<_, anyhow::Error>(());
            }
        }

        let _ = tx.send(Event::EOF);
        Ok(())
    });

    for input in rx {
        node.handle(input, &mut stdout)
            .context("Node cannot handle message")?;
    }

    handle.join().expect("STDIN thread panicked")?;
    Ok(())
}
