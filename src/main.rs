use std::io::BufRead;

use anyhow::Context;
use rustegan::{
    message::{Event, Message, Payload},
    node::Node,
};

fn main() -> anyhow::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();

    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    // read init message
    let init_msg: Message = serde_json::from_str(
        &stdin
            .next()
            .expect("no init message received")
            .context("cannot read init msg from STDIN")?,
    )
    .context("cannot deserialize init message")?;

    let Payload::Init { node_id, node_ids } = init_msg.body.payload else {
        panic!("first message should be init message");
    };

    let mut node = Node::new(node_id, node_ids, tx.clone());
    node.handle_init(
        init_msg.src,
        init_msg.dest,
        init_msg.body.msg_id,
        &mut stdout,
    )?;

    drop(stdin);
    let handle = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for line in stdin.lines() {
            let input = line.context("Maelstrom input from STDIN could not be read")?;
            let msg: Message = serde_json::from_str(&input)
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
