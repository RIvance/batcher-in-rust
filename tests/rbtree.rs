#![feature(box_patterns)]

use std::fmt::Debug;
use std::future::Future;
use batcher::batcher::{Batched, BatchedOp, WrappedOp};
use batcher::utils;

#[derive(Clone)]
pub struct RBTreeMap<K: PartialOrd, V> {
    root: Option<Box<Node<K, V>>>,
}

#[derive(Copy, Clone)]
enum Color { Red, Black }

#[derive(Clone)]
struct Node<K: PartialOrd, V> {
    key: K,
    value: V,
    color: Color,
    left: Option<Box<Node<K, V>>>,
    right: Option<Box<Node<K, V>>>,
}

impl<K: PartialOrd, V> Default for RBTreeMap<K, V> {
    fn default() -> Self { Self::new() }
}

impl<K: PartialOrd, V> RBTreeMap<K, V> {
    pub fn new() -> Self {
        Self { root: None }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.root = Some(Box::new(Self::balance(Self::insert_into(self.root.take(), key, value))));
    }

    fn insert_into(node: Option<Box<Node<K, V>>>, key: K, value: V) -> Node<K, V> {
        match node {
            None => Node { color: Color::Red, key, value, left: None, right: None },
            Some(node) => {
                let node = *node;
                if key < node.key {
                    Node { left: Some(Box::new(Self::insert_into(node.left, key, value))), ..node }
                } else if key > node.key {
                    Node { right: Some(Box::new(Self::insert_into(node.right, key, value))), ..node }
                } else {
                    Node { value, ..node }
                }
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.find(&self.root, key)
    }

    fn find<'a>(&'a self, node: &'a Option<Box<Node<K, V>>>, key: &K) -> Option<&V> {
        node.as_ref().and_then(|node| {
            if key < &node.key {
                self.find(&node.left, key)
            } else if key > &node.key {
                self.find(&node.right, key)
            } else {
                Some(&node.value)
            }
        })
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let (root, value) = Self::remove_node(self.root.take(), key);
        self.root = root.map(|node| Box::new(Self::balance(*node)));
        value
    }

    fn remove_node(node: Option<Box<Node<K, V>>>, key: &K) -> (Option<Box<Node<K, V>>>, Option<V>) {
        match node {
            None => (node, None),
            Some(node) => {
                let Node {
                    left: node_left, right: node_right,
                    key: node_key, value: node_value, color: node_color,
                } = *node;
                if key < &node_key {
                    let (updated_left, value) = Self::remove_node(node_left, key);
                    (Some(Box::new(Node {
                        left: updated_left, right: node_right,
                        color: node_color, key: node_key, value: node_value
                    })), value)
                } else if key > &node_key {
                    let (updated_right, value) = Self::remove_node(node_right, key);
                    (Some(Box::new(Node {
                        left: node_left, right: updated_right,
                        color: node_color, key: node_key, value: node_value
                    })), value)
                } else {
                    match (node_left, node_right) {
                        (None, None) => (None, Some(node_value)),
                        (Some(left), None) => (Some(left), Some(node_value)),
                        (None, Some(right)) => (Some(right), Some(node_value)),
                        (Some(left), Some(right)) => {
                            let mut left = Some(left);
                            let pred = Self::get_highest(&mut left).unwrap();
                            let left = Self::remove_from_pred(left, &pred.key);
                            (Some(Box::new(Node {
                                left, right: Some(right), color: node_color,
                                key: pred.key, value: pred.value
                            })), Some(node_value))
                        }
                    }
                }
            }
        }
    }

    fn remove_from_pred(node: Option<Box<Node<K, V>>>, pred_key: &K) -> Option<Box<Node<K, V>>> {
        match node {
            None => None,
            Some(node) => {
                let node = *node;
                if &node.key != pred_key {
                    Some(Box::new(Node {
                        left: Self::remove_from_pred(node.left, pred_key),
                        ..node
                    }))
                } else { node.left }
            }
        }
    }

    fn get_highest(node: &mut Option<Box<Node<K, V>>>) -> Option<Node<K, V>> {
        if let Some(ref mut node_ref) = node {
            if node_ref.right.is_none() {
                let left_node = node_ref.left.take();
                match left_node { 
                    Some(left_node) => node.replace(left_node),
                    None => node.take(),
                }.map(|node| *node)
            } else {
                Self::get_highest(&mut node_ref.right)
            }
        } else { None }
    }

    fn balance(node: Node<K, V>) -> Node<K, V> {
        match node {
            Node {
                color: Color::Black, key: right_key, value: right_value,
                left: Some(box Node {
                    color: Color::Red, key: top_key, value: top_value,
                    left: Some(box Node {
                        color: Color::Red, key: left_key, value: left_value,
                        left: left_left, right: left_right,
                    }), right: right_left,
                }), right: right_right,
            } => Node {
                color: Color::Red, key: top_key, value: top_value,
                left: Some(Box::new(Node {
                    color: Color::Black, key: left_key, value: left_value,
                    left: left_left, right: left_right,
                })),
                right: Some(Box::new(Node {
                    color: Color::Black, key: right_key, value: right_value,
                    left: right_left, right: right_right,
                })),
            },
            Node {
                color: Color::Black, key: right_key, value: right_value,
                left: Some(box Node {
                    color: Color::Red, key: left_key, value: left_value,
                    left: left_left, right: Some(box Node {
                        color: Color::Red, key: top_key, value: top_value,
                        left: left_right, right: right_left,
                    }),
                }), right: right_right,
            } => Node {
                color: Color::Red, key: top_key, value: top_value,
                left: Some(Box::new(Node {
                    color: Color::Black, key: left_key,
                    value: left_value, left: left_left, right: left_right,
                })),
                right: Some(Box::new(Node {
                    color: Color::Black, key: right_key, value: right_value,
                    left: right_left, right: right_right,
                })),
            },
            Node {
                color: Color::Black, key: left_key, value: left_value,
                left: left_left, right: Some(box Node {
                color: Color::Red, key: right_key, value: right_value,
                left: Some(box Node {
                    color: Color::Red, key: top_key, value: top_value,
                    left: left_right, right: right_left,
                }), right: right_right,
            }),
            } => Node {
                color: Color::Red, key: top_key, value: top_value,
                left: Some(Box::new(Node {
                    color: Color::Black, key: left_key, value: left_value,
                    left: left_left, right: left_right,
                })),
                right: Some(Box::new(Node {
                    color: Color::Black, key: right_key, value: right_value,
                    left: right_left, right: right_right,
                })),
            },
            Node {
                color: Color::Black, key: left_key, value: left_value,
                left: left_left, right: Some(box Node {
                color: Color::Red, key: top_key, value: top_value,
                left: left_right, right: Some(box Node {
                    color: Color::Red, key: right_key, value: right_value,
                    left: right_left, right: right_right,
                }),
            }),
            } => Node {
                color: Color::Red, key: top_key, value: top_value,
                left: Some(Box::new(Node {
                    color: Color::Black, key: left_key, value: left_value,
                    left: left_left, right: left_right,
                })),
                right: Some(Box::new(Node {
                    color: Color::Black, key: right_key, value: right_value,
                    left: right_left, right: right_right,
                })),
            },
            _ => node,
        }
    }
}

#[derive(Debug)]
pub enum RBTreeMapOp<K, V> {
    Insert(K, V),
    Get(K),
    Remove(K),
}

impl<K: PartialOrd, V: Send + 'static> BatchedOp for RBTreeMapOp<K, V> {
    type Res = Option<V>;
}

impl<K, V> Batched for RBTreeMap<K, V>
where
    K: PartialOrd + Debug + Send + 'static,
    V: Send + Debug + 'static,
{
    type Op = RBTreeMapOp<K, V>;

    fn init() -> Self { Self::new() }

    async fn run_batch(&mut self, ops: Vec<WrappedOp<Self::Op>>) {
        fn reduce<K, V>(l: RBTreeMap<K, V>, r: RBTreeMap<K, V>) -> RBTreeMap<K, V>
        where
            K: PartialOrd + Debug + Send + 'static,
            V: Send + Debug + 'static,
        { 
            todo!()
        }
        
        let map = move |op: WrappedOp<RBTreeMapOp<K, V>>| -> RBTreeMap<K, V> {
            match op.0 {
                RBTreeMapOp::Insert(key, value) => todo!(),
                RBTreeMapOp::Get(_) => todo!(),
                RBTreeMapOp::Remove(_) => todo!()
            }
        };
        utils::parallel_reduce(ops, reduce, map).await;
        // TODO: merge the RBTree
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut map = RBTreeMap::new();
        map.insert(4, "value4");
        map.insert(5, "value5");
        map.insert(3, "value3");
        map.insert(1, "value1");
        map.insert(2, "value2");
        assert_eq!(map.get(&1), Some(&"value1"));
        assert_eq!(map.get(&2), Some(&"value2"));
        assert_eq!(map.get(&4), Some(&"value4"));
        assert_eq!(map.get(&3), Some(&"value3"));
        assert_eq!(map.get(&5), Some(&"value5"));
        assert_eq!(map.get(&6), None);
    }

    #[test]
    fn test_insert_overwrite() {
        let mut map = RBTreeMap::new();
        map.insert(10, "value10");
        assert_eq!(map.get(&10), Some(&"value10"));

        // Insert with the same key but different value
        map.insert(10, "new_value10");
        assert_eq!(map.get(&10), Some(&"new_value10"));
    }

    #[test]
    fn test_remove() {
        let mut map = RBTreeMap::new();
        map.insert(10, "value10");
        map.insert(20, "value20");
        map.insert(5, "value5");

        assert_eq!(map.remove(&10), Some("value10"));
        assert_eq!(map.get(&10), None);

        assert_eq!(map.remove(&5), Some("value5"));
        assert_eq!(map.get(&5), None);

        assert_eq!(map.remove(&20), Some("value20"));
        assert_eq!(map.get(&20), None);

        // Try to remove a non-existent key
        assert_eq!(map.remove(&30), None);
    }

    #[test]
    fn test_remove_from_empty() {
        let mut map: RBTreeMap<i32, &str> = RBTreeMap::new();
        assert_eq!(map.remove(&10), None);
    }

    #[test]
    fn test_get_empty() {
        let map: RBTreeMap<i32, &str> = RBTreeMap::new();
        assert_eq!(map.get(&10), None);
    }

    #[test]
    fn test_insert_and_remove_sequential() {
        let mut map = RBTreeMap::new();

        for i in 0..100 {
            map.insert(i, i * 2);
            assert_eq!(map.get(&i), Some(&(i * 2)));
        }

        for i in 0..100 {
            assert_eq!(map.remove(&i), Some(i * 2));
            assert_eq!(map.get(&i), None);
        }
    }

    struct SimpleRng {
        seed: u64,
    }

    impl SimpleRng {
        const A: u64 = 6364136223846793005;
        const C: u64 = 1;

        fn new(seed: u64) -> Self { SimpleRng { seed } }

        fn next(&mut self) -> u64 {
            self.seed = self.seed.wrapping_mul(Self::A).wrapping_add(Self::C);
            self.seed
        }

        fn gen_range(&mut self, min: u64, max: u64) -> u64 {
            min + (self.next() % (max - min))
        }
    }

    #[test]
    fn test_random_inserts_and_gets() {
        let mut map = RBTreeMap::new();
        let mut rng = SimpleRng::new(114514);
        let mut keys = Vec::new();

        // Insert 1000 pseudo-random key-value pairs
        for _ in 0 .. 1000 {
            let key = rng.gen_range(0, 50000);
            let value = rng.gen_range(0, 500000);
            if keys.iter().any(|(k, _)| k == &key) {
                continue;
            }
            map.insert(key, value);
            keys.push((key, value));
        }
        
        // Check if all inserted keys return the correct values
        for (key, value) in keys {
            assert_eq!(map.get(&key), Some(&value));
        }
    }

    #[test]
    fn test_random_inserts_and_removals() {
        let mut map = RBTreeMap::new();
        let mut rng = SimpleRng::new(1919);
        let mut keys = Vec::new();

        // Insert 1000 pseudo-random key-value pairs
        for _ in 0 .. 1000 {
            let key = rng.gen_range(0, 50000);
            let value = rng.gen_range(0, 500000);
            if keys.iter().any(|(k, _)| k == &key) {
                continue;
            }
            map.insert(key, value);
            keys.push((key, value));
        }

        // Randomly remove keys
        for (key, value) in keys {
            assert_eq!(map.get(&key), Some(&value));
            assert_eq!(map.remove(&key), Some(value));
            assert_eq!(map.get(&key), None);  // Ensure the key is removed
        }

        // Ensure the map is empty after all removals
        assert_eq!(map.get(&rng.gen_range(0, 50000)), None);
    }

    #[test]
    fn test_random_inserts_gets_and_removals_stress() {
        let mut map = RBTreeMap::new();
        let mut rng = SimpleRng::new(810);
        let mut keys = Vec::new();

        // Insert 10,000 pseudo-random key-value pairs
        for _ in 0 .. 10_000 {
            let key = rng.gen_range(0, 50000);
            let value = rng.gen_range(0, 500000);
            if keys.iter().any(|(k, _)| k == &key) {
                continue;
            }
            map.insert(key, value);
            keys.push((key, value));
        }

        // Randomly get and remove 5,000 entries
        for _ in (0 .. 5000).rev() {
            let index = rng.gen_range(0, keys.len() as u64) as usize;
            let (key, value) = keys.remove(index);
            assert_eq!(map.get(&key), Some(&value));
            assert_eq!(map.remove(&key), Some(value));
            assert_eq!(map.get(&key), None);  // Ensure the key is removed
        }

        // Check remaining keys
        for (key, value) in keys {
            assert_eq!(map.get(&key), Some(&value));
        }
    }
}
