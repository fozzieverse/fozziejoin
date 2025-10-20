#[cfg(test)]
mod tests {
    use crate::stringdist::StringDistMethod;
    use crate::utils::get_pool;
    use anyhow::Result;

    #[test]
    fn test_basic_match() -> Result<()> {
        let pool = get_pool(Some(1)).expect("error setting up thread pool");
        let method = StringDistMethod::new("jaccard").expect("ohno");
        let left = vec![Some("apple".to_string())];
        let right = vec![Some("apples".to_string())];

        let matches = method.fuzzy_indices(&left, &right, &0.5, &Some(2), None, None, &pool)?;

        assert_eq!(matches.len(), 1);

        let (l, r, dist) = matches[0];
        assert_eq!(l, 0);
        assert_eq!(r, 0);
        assert!(dist <= 0.5);
        Ok(())
    }

    #[test]
    fn test_no_match_due_to_distance() -> Result<()> {
        let pool = get_pool(Some(1)).expect("error setting up thread pool");
        let method = StringDistMethod::new("jaccard").expect("ohno");

        let left = vec![Some("apple".to_string())];
        let right = vec![Some("banana".to_string())];
        let matches = method.fuzzy_indices(&left, &right, &0.2, &Some(2), None, None, &pool)?;

        assert!(matches.is_empty());
        Ok(())
    }

    #[test]
    fn test_multiple_matches() -> Result<()> {
        let pool = get_pool(Some(1)).expect("error setting up thread pool");
        let method = StringDistMethod::new("jaccard").expect("ohno");

        let left = vec![Some("apple".to_string()), Some("banana".to_string())];
        let right = vec![Some("apples".to_string()), Some("bananas".to_string())];
        let matches = method.fuzzy_indices(&left, &right, &0.5, &Some(2), None, None, &pool)?;

        assert_eq!(matches.len(), 2);
        Ok(())
    }

    #[test]
    fn test_qgram_effect() -> Result<()> {
        let pool = get_pool(Some(1)).expect("error setting up thread pool");
        let method = StringDistMethod::new("jaccard").expect("ohno");

        let left = vec![Some("abcdef".to_string())];
        let right = vec![Some("abcxyz".to_string())];

        let matches_q2 = method.fuzzy_indices(&left, &right, &0.8, &Some(2), None, None, &pool)?;
        assert_eq!(matches_q2.len(), 1);

        let matches_q3 = method.fuzzy_indices(&left, &right, &0.8, &Some(3), None, None, &pool)?;
        assert_eq!(matches_q3.len(), 0);
        Ok(())
    }

    #[test]
    fn test_small_str() -> Result<()> {
        let pool = get_pool(Some(1)).expect("error setting up thread pool");
        let method = StringDistMethod::new("jaccard").expect("ohno");

        let left = vec![Some("ab".to_string())];
        let right = vec![Some("ab".to_string())];
        let matches_q3 = method.fuzzy_indices(&left, &right, &0.8, &Some(3), None, None, &pool)?;
        assert_eq!(matches_q3.len(), 0);
        Ok(())
    }
}
