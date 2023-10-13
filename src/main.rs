use std::io::BufRead;

use anyhow::Context;
use rustegan::{
    message::{Message, Payload},
    node::Node,
};

fn main() -> anyhow::Result<()> {
    // let (tx, rx) = std::sync::mpsc::channel();

    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    // let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
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

    let mut node = Node::new(node_id, node_ids);
    node.handle_init(
        init_msg.src,
        init_msg.dest,
        init_msg.body.msg_id,
        &mut stdout,
    )?;

    while let Some(input) = stdin.next() {
        let input = input.context("Maelstrom input from STDIN could not be read")?;
        let msg: Message = serde_json::from_str(&input)
            .context("Maelstrom input from STDIN could not be deserialized")?;

        node.handle(msg, &mut stdout)
            .context("Node cannot handle message")?;
    }

    Ok(())
}
