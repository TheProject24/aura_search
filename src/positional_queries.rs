#[derive(Debug, Clone)]
pub struct PositionalPosting {
    pub doc_id: u32,
    pub positions: Vec<u32>,
}

pub fn is_exact_phrase_match(
    word_one_addresses: &[u32],
    word_two_addresses: &[u32]
) -> bool {
    let mut pointer_one = 0;
    let mut pointer_two = 0;

    while pointer_one < word_one_addresses.len() && pointer_two < word_two_addresses.len() {
        let pos_one = word_one_addresses[pointer_one];
        let pos_two = word_two_addresses[pointer_two];

        if pos_two == pos_one + 1 {
            return true;
        }
        if pos_one < pos_two {
            pointer_one += 1;
        } else {
            pointer_two += 1;
        }
    }

    false
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_exact_phrase_matching() {
        let hot_positions_doc1 = vec![5];
        let dog_positions_doc1 = vec![2];

        let is_match_1 = is_exact_phrase_match(&hot_positions_doc1, &dog_positions_doc1);

        assert_eq!(is_match_1, false);
        let hot_positions_doc2 = vec![2, 6];
        let dog_position_doc2 = vec![7];

        let is_match_2 = is_exact_phrase_match(&hot_positions_doc2, &dog_position_doc2);

        assert_eq!(is_match_2, true);

        println!("Phrase detection completed successfully!");
    }
}