use data_bucket::Link;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use std::fmt::Debug;

pub fn validate_events<T>(evs: &mut Vec<ChangeEvent<Pair<T, Link>>>) -> Vec<ChangeEvent<Pair<T, Link>>>
where
    T: Debug,
{
    let mut removed_events = vec![];

    while let Some(first_invalid_pos) = find_first_gap(evs) {
        removed_events.extend(evs.split_off(first_invalid_pos));
    }

    removed_events.sort_by_key(|ev2| std::cmp::Reverse(ev2.id()));

    removed_events
}

fn find_first_gap<T>(evs: &[ChangeEvent<Pair<T, Link>>]) -> Option<usize> {
    if evs.len() < 2 {
        return None;
    }

    let first_id = evs[0].id().inner();
    let last_id = evs.last().unwrap().id().inner();
    let expected_span = (evs.len() - 1) as u64;

    if last_id.checked_sub(first_id) == Some(expected_span) {
        return None;
    }

    // Event ids are unique by the indexset CDC contract. After sorting, the
    // expected id at position `pos` is therefore a monotonic predicate: it is
    // true through the contiguous prefix and false after its first gap.
    let mut left = 1;
    let mut right = evs.len();
    while left < right {
        let pos = left + (right - left) / 2;
        let expected_id = first_id.checked_add(pos as u64);
        if expected_id == Some(evs[pos].id().inner()) {
            left = pos + 1;
        } else {
            right = pos;
        }
    }

    debug_assert!(left < evs.len());
    Some(left)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert_at(id: u64) -> ChangeEvent<Pair<u64, Link>> {
        ChangeEvent::InsertAt {
            event_id: id.into(),
            max_value: Pair {
                key: id,
                value: Link::default(),
            },
            value: Pair {
                key: id,
                value: Link::default(),
            },
            index: 0,
        }
    }

    #[test]
    fn detects_gap_behind_long_contiguous_tail() {
        // The contiguous tail is longer than the old bounded scan window.
        let mut evs: Vec<_> = (100..140).map(insert_at).collect();
        evs.extend((1000..1050).map(insert_at));

        let removed = validate_events(&mut evs);

        assert_eq!(evs.len(), 40);
        assert!(evs.iter().all(|ev| ev.id() < 140.into()));
        assert_eq!(removed.len(), 50);
        assert!(removed.iter().all(|ev| ev.id() >= 1000.into()));
    }

    #[test]
    fn keeps_gapless_stream_untouched() {
        let mut evs: Vec<_> = (100..200).map(insert_at).collect();
        let removed = validate_events(&mut evs);

        assert!(removed.is_empty());
        assert_eq!(evs.len(), 100);
    }

    #[test]
    fn finds_gap_after_short_prefix() {
        let mut evs: Vec<_> = (100..105).map(insert_at).collect();
        evs.extend((1_000..2_000).map(insert_at));

        let removed = validate_events(&mut evs);

        assert_eq!(evs.len(), 5);
        assert_eq!(removed.len(), 1_000);
        assert_eq!(removed.first().unwrap().id(), 1_999.into());
        assert_eq!(removed.last().unwrap().id(), 1_000.into());
    }

    #[test]
    fn finds_gap_at_first_position() {
        let mut evs = vec![insert_at(100)];
        evs.extend((102..200).map(insert_at));

        let removed = validate_events(&mut evs);

        assert_eq!(evs.len(), 1);
        assert_eq!(removed.len(), 98);
        assert_eq!(removed.first().unwrap().id(), 199.into());
        assert_eq!(removed.last().unwrap().id(), 102.into());
    }

    #[test]
    fn finds_gap_at_last_position() {
        let mut evs: Vec<_> = (100..200).map(insert_at).collect();
        evs.push(insert_at(202));

        let removed = validate_events(&mut evs);

        assert_eq!(evs.len(), 100);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].id(), 202.into());
    }

    #[test]
    fn handles_ids_near_u64_boundary() {
        let first_id = u64::MAX - 3;
        let mut gapless = (0..4).map(|offset| insert_at(first_id + offset)).collect();
        let removed = validate_events(&mut gapless);

        assert!(removed.is_empty());
        assert_eq!(gapless.len(), 4);

        let mut gapped = vec![insert_at(first_id), insert_at(first_id + 1), insert_at(u64::MAX)];
        let removed = validate_events(&mut gapped);

        assert_eq!(gapped.len(), 2);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].id(), u64::MAX.into());
    }

    #[test]
    fn finds_gap_at_every_position() {
        for len in 2..=128 {
            for gap_pos in 1..len {
                let mut evs: Vec<_> = (0..len)
                    .map(|pos| {
                        let id = 1_000 + pos as u64 + if pos >= gap_pos { 1 } else { 0 };
                        insert_at(id)
                    })
                    .collect();

                let removed = validate_events(&mut evs);

                assert_eq!(evs.len(), gap_pos);
                assert_eq!(removed.len(), len - gap_pos);
            }
        }
    }
}
