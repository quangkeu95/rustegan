use anyhow::Context;
use rustegan::{message::Message, node::Node};

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut stdout = std::io::stdout().lock();

    let mut node = Node::new("n0");

    for input in inputs {
        let input = input.context("Maelstrom input from STDIN could not be deserialize")?;

        node.handle(input, &mut stdout)
            .context("Node cannot handle message")?;
    }

    Ok(())
}
