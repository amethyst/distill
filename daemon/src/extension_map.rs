use std::{borrow::Borrow, collections::HashMap, hash::Hash, path::Path};

struct SequenceTrie<K, V> {
    value: Option<V>,
    children: HashMap<K, SequenceTrie<K, V>>,
}
impl<K, V> Default for SequenceTrie<K, V> {
    fn default() -> Self {
        Self {
            value: Default::default(),
            children: Default::default(),
        }
    }
}
impl<K: Eq + Hash, V> SequenceTrie<K, V> {
    fn insert(&mut self, key: impl Iterator<Item = K>, value: V) -> Option<V> {
        let node = key.fold(self, |node, k| {
            node.children.entry(k).or_insert_with(SequenceTrie::default)
        });
        std::mem::replace(&mut node.value, Some(value))
    }

    fn get<'a, Q: ?Sized, I>(&self, key: I) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + 'a,
        I: Iterator<Item = &'a Q>,
    {
        let mut current = self;
        for fragment in key {
            match current.children.get(fragment.borrow()) {
                Some(node) => current = node,
                None => return None,
            }
        }
        current.value.as_ref()
    }
}

pub struct ExtensionMap<T> {
    map: SequenceTrie<String, usize>,
    values: Vec<T>,
}
impl<T> Default for ExtensionMap<T> {
    fn default() -> Self {
        Self {
            map: Default::default(),
            values: Default::default(),
        }
    }
}

impl<T> ExtensionMap<T> {
    pub fn insert(&mut self, extensions: &[&str], value: T) {
        let idx = self.values.len();
        self.values.push(value);

        extensions
            .iter()
            .for_each(|extension| self.insert_inner(extension.as_ref(), idx));
    }

    fn insert_inner(&mut self, extension: &str, idx: usize) {
        let key = extension.rsplit('.').map(|e| e.to_lowercase());
        let already_in = self.map.insert(key, idx).is_some();
        if already_in {
            panic!("extension '{}' already present", extension);
        }
    }

    pub fn get(&self, path: &Path) -> Option<&T> {
        let path = path.to_str().expect("non-utf8 path").to_lowercase();
        let mut extension = path.split('.');

        let idx = loop {
            match self.map.get(extension.clone().rev()) {
                Some(i) => break *i,
                None => {
                    extension.next()?;
                }
            }
        };

        Some(&self.values[idx])
    }
}
#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::extension_map::ExtensionMap;

    #[test]
    fn single() {
        let mut map = ExtensionMap::default();
        map.insert(&["txt"], "text");

        assert_eq!(map.get(Path::new("no")), None);
        assert_eq!(map.get(Path::new("test.txt")), Some(&"text"));
        assert_eq!(map.get(Path::new("test.my.txt")), Some(&"text"));
    }

    #[test]
    fn specificity() {
        let mut map = ExtensionMap::default();
        map.insert(&["ron"], "RON");
        map.insert(&["scn.ron", "scn"], "SCENE");
        map.insert(&["this.scn.ron"], "THIS");

        assert_eq!(map.get(Path::new("file.ron")), Some(&"RON"));
        assert_eq!(map.get(Path::new("file.scn")), Some(&"SCENE"));
        assert_eq!(map.get(Path::new("file.scn.ron")), Some(&"SCENE"));
        assert_eq!(map.get(Path::new("this.scn.ron")), Some(&"THIS"));
    }

    #[test]
    fn alias() {
        let mut map = ExtensionMap::default();
        map.insert(&["this", "that"], ());

        assert_eq!(map.get(Path::new("a.this")), Some(&()));
        assert_eq!(map.get(Path::new("a.that")), Some(&()));
    }

    #[test]
    fn case_insensitive() {
        let mut map = ExtensionMap::default();
        map.insert(&["eXteNsIon"], ());

        assert_eq!(map.get(Path::new("a.extension")), Some(&()));
        assert_eq!(map.get(Path::new("a.EXTENSION")), Some(&()));
    }
}
