use super::SortKey;
use std::time::Duration;

#[cfg(test)]
#[test]
fn sort_key_sort_order() {
    assert!(
        SortKey::new(None, i64::max_value())
            < SortKey::new(Some(Duration::default()), i64::min_value())
    );
}
