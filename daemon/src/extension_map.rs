use sequence_trie::SequenceTrie;
use std::path::Path;

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
        let already_in = self.map.insert_owned(key, idx).is_some();
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
