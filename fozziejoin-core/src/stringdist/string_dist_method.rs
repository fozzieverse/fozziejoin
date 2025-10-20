use crate::stringdist::jaccard::Jaccard;

pub enum StringDistMethod {
    Jaccard {},
}

impl StringDistMethod {
    pub fn new(method: &str) -> Result<StringDistMethod, anyhow::Error> {
        match method {
            "jaccard" => Ok(StringDistMethod::Jaccard {}),
            _ => Err(anyhow::anyhow!("Unsupported method `{}`", method)),
        }
    }

    pub fn fuzzy_indices(
        &self,
        left: &Vec<Option<String>>,
        right: &Vec<Option<String>>,
        max_distance: &f64,
        q: &Option<usize>,
        prefix_weight: Option<f64>,
        max_prefix: Option<usize>,
        pool: &rayon::ThreadPool,
    ) -> anyhow::Result<Vec<(usize, usize, f64)>> {
        let result = match self {
            StringDistMethod::Jaccard {} => Jaccard.fuzzy_indices(
                left,
                right,
                max_distance,
                q,
                prefix_weight,
                max_prefix,
                pool,
            ),
        }?;

        Ok(result)
    }
}
