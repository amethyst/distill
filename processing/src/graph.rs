
use daggy::{Dag};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::fmt;
use crate::processor::{self, Processor, AnyProcessor, TypeId};

type NodeId = u32;
type ArgIndex = usize;
type ArgId = (NodeId, ArgIndex);
type NodeRef = daggy::NodeIndex;
type NodeGraph = Dag<Node, NodeEdge, u32>;

#[derive(Copy, Clone, Debug)]
pub struct NodeEdge {
    from: ArgId,
    to: ArgId,
}
impl NodeEdge {
    pub fn new(from_node: NodeId, from_arg: ArgIndex, to_node: NodeId, to_arg: ArgIndex) -> NodeEdge {
        NodeEdge { from: (from_node, from_arg), to: (to_node, to_arg) }
    }
}
pub struct Node {
    id: NodeId,
    processor: Box<AnyProcessor>,
}
impl Node {
    pub fn new(id: NodeId, processor: Box<AnyProcessor>) -> Node {
        Node { id, processor }
    }
    pub fn from_processor<T: Processor + 'static>(id: NodeId) -> Node {
        Node { id, processor: Box::new(processor::into_any::<T>()) }
    }
    pub fn make_edge(from: &Node, from_arg: &'static str, to: &Node, to_arg: &'static str) -> Result<NodeEdge> {
        let mut from_idx = None;
        for (idx, name) in from.processor.output_names().iter().enumerate() {
            if *name == from_arg {
                from_idx = Some(idx);
            }
        }
        if None == from_idx {
            return Err(Error::ArgNameNotFound(from.id, from_arg))
        }
        let mut to_idx = None;
        for (idx, name) in to.processor.input_names().iter().enumerate() {
            if *name == to_arg {
                to_idx = Some(idx);
            }
        }
        if None == to_idx {
            return Err(Error::ArgNameNotFound(to.id, to_arg))
        }
        Ok(NodeEdge { from: (from.id, from_idx.unwrap()), to: (to.id, to_idx.unwrap()) })
    }
}

pub struct Graph {
    graph: NodeGraph,
    nodes: HashMap<NodeId, NodeRef>,
}
pub struct GraphBuilder {
    nodes: Vec<Node>,
    edges: Vec<NodeEdge>,
}


pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug)]
pub enum Error {
    SelfReference(NodeEdge),
    ArgNotFound(NodeEdge),
    NodeNotFound(NodeEdge),
    TypeMismatch(NodeEdge, TypeId, TypeId),
    GraphCycle(NodeEdge),
    ArgNameNotFound(NodeId, &'static str),
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::SelfReference(_) => "Node referenced itself",
            Error::ArgNotFound(_) => "Node argument not found",
            Error::NodeNotFound(_) => "Node not found",
            Error::TypeMismatch(_, _, _) => "Node argument type mismatch",
            Error::GraphCycle(_) => "Node argument type mismatch",
            Error::ArgNameNotFound(_, _) => "Node argument name not found",
        }
    }

    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            Error::SelfReference(_) => None,
            Error::ArgNotFound(_) => None,
            Error::NodeNotFound(_) => None,
            Error::TypeMismatch(_, _, _) => None,
            Error::GraphCycle(_) => None,
            Error::ArgNameNotFound(_, _) => None,
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            _ => { Ok(())}
            // Error::SelfReference(_) => "Node referenced itself",
            // Error::ArgNotFound(_) => "Node argument not found",
            // Error::NodeNotFound(_) => "Node not found",
            // Error::TypeMismatch(_, _, _) => "Node argument type mismatch",
            // Error::GraphCycle(_) => "Node argument type mismatch",
        }
    }
}

impl From<daggy::WouldCycle<NodeEdge>> for Error {
    fn from(err: daggy::WouldCycle<NodeEdge>) -> Error {
        Error::GraphCycle(err.0)
    }
}

impl GraphBuilder {
    pub fn new() -> Self {
        GraphBuilder { nodes: Vec::new(), edges: Vec::new() }
    }
    pub fn add_node(mut self, node: Node) -> Self {
        self.nodes.push(node);
        self
    }
    pub fn add_edge(mut self, edge: NodeEdge) -> Self {
        self.edges.push(edge);
        self
    }

    pub fn build(self) -> Result<Graph> {
        let mut nodes_by_id = HashMap::new();
        for node in self.nodes.iter() {
            nodes_by_id.insert(node.id, node);
        }
        for edge in self.edges.iter() {
            let from_node = nodes_by_id.get(&edge.from.0);
            if let None = from_node {
                return Err(Error::NodeNotFound(*edge));
            }
            let from_node = from_node.unwrap();
            let to_node = nodes_by_id.get(&edge.to.0);
            if let None = to_node {
                return Err(Error::NodeNotFound(*edge));
            }
            let to_node = to_node.unwrap();
            if from_node.id == to_node.id {
                return Err(Error::SelfReference(*edge))
            }
            let outputs = from_node.processor.outputs();
            let inputs = to_node.processor.inputs();
            if edge.from.1 >= outputs.len() {
                return Err(Error::ArgNotFound(*edge))
            }
            if edge.to.1 >= inputs.len() {
                return Err(Error::ArgNotFound(*edge))
            }
            if outputs[edge.from.1] != inputs[edge.to.1] {
                let from_type = outputs[edge.from.1].clone();
                let to_type = inputs[edge.to.1].clone();
                return Err(Error::TypeMismatch(*edge, from_type, to_type))
            }
        }
        let mut graph = NodeGraph::new();
        let mut node_refs = HashMap::new();
        for node in self.nodes {
            node_refs.insert(node.id, graph.add_node(node));
        }
        for edge in self.edges {
            graph.add_edge(node_refs[&edge.from.0], node_refs[&edge.to.0], edge)?;
        }
        Ok(Graph { graph: graph, nodes: node_refs })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_dyn::uuid;
    use crate::processor::{self, Arg, Processor, ProcessorValues, RunNow};
    use downcast::Downcast;

    uuid!{
        First => 14092692613983100637224012401022025107,
        Second => 14092692613983100637224012401022025108
    }

    struct First;
    impl Processor for First {
        fn name() -> &'static str { "First" }
        fn input_names() -> &'static [&'static str] { &["f", "b"] }
        fn output_names() -> &'static [&'static str] { &["g", "c"] }
        type Inputs = (Vec<Arg<f32>>, Arg<u8>);
        type Outputs = (Arg<u32>, Vec<Arg<u16>>);
        fn run((f, b): &Self::Inputs) -> Self::Outputs {
            let mut total = 0u32;
            for x in f.iter() {
                total += **x as u32;
            }
            total += **b as u32;
            (Arg::from(total), vec![Arg::from(88u16)])
        }
    }
    struct Second;
    impl Processor for Second {
        fn name() -> &'static str { "Second" }
        fn input_names() -> &'static [&'static str] { &["f", "b"] }
        fn output_names() -> &'static [&'static str] { &["g", "c"] }
        type Inputs = (Arg<u32>, Vec<Arg<u16>>);
        type Outputs = (Arg<u32>, Arg<u16>);
        fn run((i, _f): &Self::Inputs) -> Self::Outputs {
            let mut total = 0u32;
            total += **i as u32;
            (Arg::from(total), Arg::from(88u16))
        }
    }

    #[test]
    fn test() {
        let first_node = Node::from_processor::<First>(1);
        let second_node = Node::from_processor::<Second>(2);
        let edge1 = Node::make_edge(&first_node, "g", &second_node, "f").unwrap();
        let edge2 = Node::make_edge(&first_node, "c", &second_node, "b").unwrap();
        let graph = GraphBuilder::new().add_node(first_node).add_node(second_node).add_edge(edge1).add_edge(edge2).build().unwrap();
        

        let mut values = ProcessorValues::new(vec![ Some(Box::new(vec![Arg::from(3.2f32)])), Some(Box::new(2u8)) ]);
        First::run_now(&mut values);
        let out = values.outputs().get(0).unwrap();
        println!("{}", **Downcast::<Arg<u32>>::downcast_ref(&**out.as_ref().unwrap()).unwrap());
    }
}