pub fn total_combinations(alphabet_len: usize, max_length: usize) -> Option<u64> {
    if alphabet_len == 0 || max_length == 0 {
        return Some(0);
    }

    let mut total = 0_u64;
    let mut current = 1_u64;

    for _ in 0..max_length {
        current = current.checked_mul(alphabet_len as u64)?;
        total = total.checked_add(current)?;
    }

    Some(total)
}

pub fn split_evenly(total: u64, parts: usize) -> Vec<(u64, u64)> {
    if total == 0 || parts == 0 {
        return Vec::new();
    }

    let part_count = parts.min(total as usize) as u64;
    let base = total / part_count;
    let remainder = total % part_count;

    let mut ranges = Vec::with_capacity(part_count as usize);
    let mut start = 0_u64;

    for idx in 0..part_count {
        let size = base + u64::from(idx < remainder);
        let end = start + size;
        ranges.push((start, end));
        start = end;
    }

    ranges
}
