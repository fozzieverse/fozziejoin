#[cfg(test)]
mod tests {
    use crate::stringdist::Jaccard;
    use anyhow::Result;

    #[test]
    fn test_basic_match() -> Result<()> {
        let jaccard = Jaccard::new();
        let left = vec!["apple".to_string()];
        let right = vec!["apples".to_string()];
        let matches = jaccard.fuzzy_indices(&left, &right, 0.5, 2)?;

        assert_eq!(matches.len(), 1);
        let (l, r, dist) = matches[0];
        assert_eq!(l, 0);
        assert_eq!(r, 0);
        assert!(dist <= 0.5);
        Ok(())
    }

    #[test]
    fn test_no_match_due_to_distance() -> Result<()> {
        let jaccard = Jaccard::new();
        let left = vec!["apple".to_string()];
        let right = vec!["banana".to_string()];
        let matches = jaccard.fuzzy_indices(&left, &right, 0.2, 2)?;

        assert!(matches.is_empty());
        Ok(())
    }

    #[test]
    fn test_multiple_matches() -> Result<()> {
        let jaccard = Jaccard::new();
        let left = vec!["apple".to_string(), "banana".to_string()];
        let right = vec!["apples".to_string(), "bananas".to_string()];
        let matches = jaccard.fuzzy_indices(&left, &right, 0.5, 2)?;

        assert_eq!(matches.len(), 2);
        Ok(())
    }

    #[test]
    fn test_qgram_effect() -> Result<()> {
        let jaccard = Jaccard::new();
        let left = vec!["abcdef".to_string()];
        let right = vec!["abcxyz".to_string()];
        let matches_q2 = jaccard.fuzzy_indices(&left, &right, 0.8, 2)?;
        assert_eq!(matches_q2.len(), 1);
        let matches_q3 = jaccard.fuzzy_indices(&left, &right, 0.8, 3)?;
        assert_eq!(matches_q3.len(), 0);
        Ok(())
    }

    #[test]
    fn test_small_str() -> Result<()> {
        let jaccard = Jaccard::new();
        let left = vec!["ab".to_string()];
        let right = vec!["ab".to_string()];
        let matches_q3 = jaccard.fuzzy_indices(&left, &right, 0.8, 3)?;
        assert_eq!(matches_q3.len(), 0);
        Ok(())
    }
}
