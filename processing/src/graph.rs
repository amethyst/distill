
use petgraph;
use std::collections::{HashMap};
use std::marker::PhantomData;
use std::fmt;
use crate::processor::{self, Processor, AnyProcessor, TypeId, ProcessorObj, ProcessorValues};

#[derive(Copy, Ord, PartialOrd, PartialEq, Eq, Clone, Hash, Debug)]
pub struct NodeId(u32);
type ArgIndex = usize;
type ArgId = (NodeId, ArgIndex);
type NodeGraph = petgraph::graphmap::GraphMap<NodeId, NodeEdge, DirectedEdgeType>;
struct DirectedEdgeType;
impl petgraph::EdgeType for DirectedEdgeType {
    fn is_directed() -> bool { true }
}

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
    pub fn from_constants(id: NodeId, values: Vec<processor::IOData>) -> Node {
        Node { id, processor: Box::new(processor::ConstantProcessor::new(values)) }
    }
    pub fn from_processor<T: Processor + 'static>(id: NodeId) -> Node {
        Node { id, processor: Box::new(processor::into_any::<T>()) }
    }
    pub fn make_edge(from: & Node, from_arg: &'static str, to: &Node, to_arg: &'static str) -> Result<NodeEdge> {
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
    execution_order: Vec<NodeId>,
    nodes: HashMap<NodeId, Node>,
}
impl Graph {
    pub fn execute(&mut self, root: NodeId) {
        let mut outputs: HashMap<NodeId, Vec<Option<Box<ProcessorObj>>>> = HashMap::new();
        for node_id in self.execution_order.iter() {
            let mut inputs: Vec<Option<Box<ProcessorObj>>> = Vec::new();
            for (_, _, e) in self.graph.edges(*node_id)
            // .filter(|(_,_, e)| e.to.0 == *node_id) 
            
            {
                println!("input e {:?}", e);
                inputs.push(outputs[&e.from.0][e.from.1 as usize].as_ref().map(|o| o.shallow_clone()));
            }
            println!("running {}", inputs.len());
            let mut values = ProcessorValues::new(inputs);
            let node = &self.nodes[&node_id];
            node.processor.run(&mut values);
            outputs.insert(*node_id, values.drain_outputs());
        }
    }
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
    GraphCycle(NodeId),
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
        use std::error::Error as StdError;
        match *self {
            Error::SelfReference(_) => f.write_str(self.description()),
            Error::ArgNotFound(_) => f.write_str(self.description()),
            Error::NodeNotFound(_) => f.write_str(self.description()),
            Error::TypeMismatch(_, _, _) => f.write_str(self.description()),
            Error::GraphCycle(_) => f.write_str(self.description()),
            Error::ArgNameNotFound(_, _) => f.write_str(self.description()),
        }
    }
}

impl From<petgraph::algo::Cycle<NodeId>> for Error {
    fn from(err: petgraph::algo::Cycle<NodeId>) -> Error {
        Error::GraphCycle(err.node_id())
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
            let from_node = nodes_by_id.get(&edge.from.0).ok_or_else(|| Error::NodeNotFound(*edge))?;
            let to_node = nodes_by_id.get(&edge.to.0).ok_or_else(|| Error::NodeNotFound(*edge))?;
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
            graph.add_node(node.id);
            node_refs.insert(node.id, node);
        }
        for edge in self.edges {
            println!("adding edge from {:?} to {:?}",edge.from, edge.to);
            graph.add_edge(edge.from.0, edge.to.0, edge);
        }
        let sorted = petgraph::algo::toposort(&graph, None)?;
            println!("got {} edges and {} nodes", graph.edge_count(), graph.node_count());
        Ok(Graph { graph: graph, nodes: node_refs, execution_order: sorted, })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_dyn::uuid;
    use crate::processor::{self, Arg, Val, Processor, ProcessorValues, RunNow};
    use downcast::Downcast;

    uuid!{
        First => 14092692613983100637224012401022025107,
        Second => 14092692613983100637224012401022025108
    }

    struct First;
    impl Processor for First {
        fn name() -> &'static str { "First" }
        fn input_names() -> Vec<String> { vec!["i"].iter().map(|d| d.to_string()).collect() }
        fn output_names() -> Vec<String> { vec!["g", "c"].iter().map(|d| d.to_string()).collect() }
        type Inputs = (Arg<u32>);
        type Outputs = (Val<u32>, Vec<Val<u16>>);
        fn run((i): Self::Inputs) -> Self::Outputs {
            let mut total = 0u32;
            total += *i as u32;
            (Val::from(total), vec![Val::from(88u16)])
        }
    }
    struct Second;
    impl Processor for Second {
        fn name() -> &'static str { "Second" }
        fn input_names() -> Vec<String> { vec!["f", "b"].iter().map(|d| d.to_string()).collect() }
        fn output_names() -> Vec<String> { vec!["g", "c"].iter().map(|d| d.to_string()).collect() }
        type Inputs = (Arg<u32>, Vec<Arg<u16>>);
        type Outputs = (Val<u32>, Val<u16>);
        fn run((i, _f): Self::Inputs) -> Self::Outputs {
            let mut total = 0u32;
            total += *i as u32;
            (Val::from(total), Val::from(88u16))
        }
    }

    #[test]
    fn test() {
        let graph_inputs = Node::from_constants(NodeId(0), vec![ processor::IOData::new("a".to_string(), Some(Box::new(Arg::from(15u32)))) ] );
        let first_node = Node::from_processor::<First>(NodeId(1));
        let second_node = Node::from_processor::<Second>(NodeId(2));
        let edge0 = Node::make_edge(&graph_inputs, "a", &first_node, "i").unwrap();
        let edge1 = Node::make_edge(&first_node, "g", &second_node, "f").unwrap();
        let edge2 = Node::make_edge(&first_node, "c", &second_node, "b").unwrap();
        let mut graph = GraphBuilder::new().add_node(graph_inputs).add_edge(edge0).add_edge(edge1).add_edge(edge2).add_node(first_node).add_node(second_node).build().unwrap();
        graph.execute(NodeId(0));
    }
}