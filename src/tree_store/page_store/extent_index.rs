use alloc::boxed::Box;

// AVL index augmented with each subtree's largest extent. Address-ordered first-fit
// queries skip undersized subtrees, so fragmentation never turns lookup into a scan.
#[derive(Default)]
pub(super) struct ExtentIndex {
    root: Option<Box<Node>>,
}

struct Node {
    start: u32,
    len: u32,
    max_len: u32,
    height: u8,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}

impl Node {
    fn height(node: Option<&Self>) -> u8 {
        node.map_or(0, |node| node.height)
    }

    fn max_len(node: Option<&Self>) -> u32 {
        node.map_or(0, |node| node.max_len)
    }

    fn refresh(&mut self) {
        self.height =
            1 + Self::height(self.left.as_deref()).max(Self::height(self.right.as_deref()));
        self.max_len = self
            .len
            .max(Self::max_len(self.left.as_deref()))
            .max(Self::max_len(self.right.as_deref()));
    }

    fn rotate_left(root: &mut Option<Box<Self>>) {
        let mut node = root.take().unwrap();
        let mut right = node.right.take().unwrap();
        node.right = right.left.take();
        node.refresh();
        right.left = Some(node);
        right.refresh();
        *root = Some(right);
    }

    fn rotate_right(root: &mut Option<Box<Self>>) {
        let mut node = root.take().unwrap();
        let mut left = node.left.take().unwrap();
        node.left = left.right.take();
        node.refresh();
        left.right = Some(node);
        left.refresh();
        *root = Some(left);
    }

    fn balance(root: &mut Option<Box<Self>>) {
        let Some(node) = root else { return };
        node.refresh();
        if Self::height(node.left.as_deref()) > Self::height(node.right.as_deref()) + 1 {
            let left = node.left.as_ref().unwrap();
            if Self::height(left.right.as_deref()) > Self::height(left.left.as_deref()) {
                Self::rotate_left(&mut node.left);
            }
            Self::rotate_right(root);
        } else if Self::height(node.right.as_deref()) > Self::height(node.left.as_deref()) + 1 {
            let right = node.right.as_ref().unwrap();
            if Self::height(right.left.as_deref()) > Self::height(right.right.as_deref()) {
                Self::rotate_right(&mut node.right);
            }
            Self::rotate_left(root);
        }
    }

    fn insert(root: &mut Option<Box<Self>>, start: u32, len: u32) -> Option<u32> {
        let Some(node) = root else {
            *root = Some(Box::new(Self {
                start,
                len,
                max_len: len,
                height: 1,
                left: None,
                right: None,
            }));
            return None;
        };
        let previous = match start.cmp(&node.start) {
            core::cmp::Ordering::Less => Self::insert(&mut node.left, start, len),
            core::cmp::Ordering::Greater => Self::insert(&mut node.right, start, len),
            core::cmp::Ordering::Equal => Some(core::mem::replace(&mut node.len, len)),
        };
        Self::balance(root);
        previous
    }

    fn take_first(root: &mut Option<Box<Self>>) -> Option<Box<Self>> {
        let node = root.as_mut()?;
        if node.left.is_none() {
            let mut first = root.take().unwrap();
            *root = first.right.take();
            Some(first)
        } else {
            let first = Self::take_first(&mut node.left);
            Self::balance(root);
            first
        }
    }

    fn remove(root: &mut Option<Box<Self>>, start: u32) -> Option<u32> {
        let node = root.as_mut()?;
        let previous = match start.cmp(&node.start) {
            core::cmp::Ordering::Less => Self::remove(&mut node.left, start),
            core::cmp::Ordering::Greater => Self::remove(&mut node.right, start),
            core::cmp::Ordering::Equal => {
                let mut node = root.take().unwrap();
                let len = node.len;
                if let Some(mut next) = Self::take_first(&mut node.right) {
                    next.left = node.left.take();
                    next.right = node.right.take();
                    *root = Some(next);
                } else {
                    *root = node.left.take();
                }
                Some(len)
            }
        };
        Self::balance(root);
        previous
    }

    fn first_fit(&self, len: u32, lower: u32) -> Option<u32> {
        if self.max_len < len {
            return None;
        }
        if self.start < lower {
            return self.right.as_ref()?.first_fit(len, lower);
        }
        if let Some(found) = self
            .left
            .as_ref()
            .and_then(|node| node.first_fit(len, lower))
        {
            return Some(found);
        }
        if self.len >= len {
            return Some(self.start);
        }
        self.right.as_ref()?.first_fit(len, lower)
    }
}

impl ExtentIndex {
    pub(super) fn insert(&mut self, start: u32, len: u32) -> Option<u32> {
        Node::insert(&mut self.root, start, len)
    }

    pub(super) fn remove(&mut self, start: u32) -> Option<u32> {
        Node::remove(&mut self.root, start)
    }

    pub(super) fn first_fit(&self, len: u32) -> Option<u32> {
        self.first_fit_from(len, 0)
    }

    pub(super) fn first_fit_from(&self, len: u32, lower: u32) -> Option<u32> {
        self.root.as_ref()?.first_fit(len, lower)
    }

    pub(super) fn max_len(&self) -> u32 {
        Node::max_len(self.root.as_deref())
    }

    pub(super) fn floor(&self, start: u32) -> Option<(u32, u32)> {
        let mut node = self.root.as_ref();
        let mut result = None;
        while let Some(current) = node {
            if current.start <= start {
                result = Some((current.start, current.len));
                node = current.right.as_ref();
            } else {
                node = current.left.as_ref();
            }
        }
        result
    }

    pub(super) fn ceiling(&self, start: u32) -> Option<(u32, u32)> {
        let mut node = self.root.as_ref();
        let mut result = None;
        while let Some(current) = node {
            if current.start >= start {
                result = Some((current.start, current.len));
                node = current.left.as_ref();
            } else {
                node = current.right.as_ref();
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::collections::BTreeMap;

    fn validate(node: Option<&Node>, low: Option<u32>, high: Option<u32>) -> (u8, u32) {
        let Some(node) = node else { return (0, 0) };
        assert!(low.is_none_or(|low| low < node.start));
        assert!(high.is_none_or(|high| node.start < high));
        let (left_height, left_max) = validate(node.left.as_deref(), low, Some(node.start));
        let (right_height, right_max) = validate(node.right.as_deref(), Some(node.start), high);
        assert!(left_height.abs_diff(right_height) <= 1);
        assert_eq!(node.height, 1 + left_height.max(right_height));
        assert_eq!(node.max_len, node.len.max(left_max).max(right_max));
        (node.height, node.max_len)
    }

    #[test]
    fn sorted_insertions_and_deletions_stay_balanced() {
        let mut index = ExtentIndex::default();
        for key in 0..1000 {
            index.insert(key, key + 1);
        }
        assert!(validate(index.root.as_deref(), None, None).0 <= 16);
        for key in (0..1000).rev() {
            assert_eq!(index.first_fit(key + 1), Some(key));
            assert_eq!(index.remove(key), Some(key + 1));
            validate(index.root.as_deref(), None, None);
        }
        assert_eq!(index.first_fit(1), None);
    }

    #[test]
    fn mixed_updates_and_bounded_queries_match_an_ordered_map() {
        let mut index = ExtentIndex::default();
        let mut reference = BTreeMap::new();
        let mut state = 941u64;
        for step in 0..4000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let key = u32::try_from(state % 256).unwrap();
            let len = u32::try_from((state >> 8) % 128).unwrap();
            if step % 3 == 0 {
                assert_eq!(index.remove(key), reference.remove(&key));
            } else {
                assert_eq!(index.insert(key, len), reference.insert(key, len));
            }
            validate(index.root.as_deref(), None, None);
            assert_eq!(
                index.max_len(),
                reference.values().copied().max().unwrap_or(0)
            );
            for requested in [1, 17, 64, 127] {
                for lower in [0, 100, 255] {
                    let expected = reference
                        .range(lower..)
                        .find(|&(_, len)| *len >= requested)
                        .map(|(&key, _)| key);
                    assert_eq!(index.first_fit_from(requested, lower), expected);
                }
            }
            assert_eq!(
                index.floor(key),
                reference.range(..=key).next_back().map(|(&k, &v)| (k, v))
            );
            assert_eq!(
                index.ceiling(key),
                reference.range(key..).next().map(|(&k, &v)| (k, v))
            );
        }
    }
}
