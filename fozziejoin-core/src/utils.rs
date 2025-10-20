use anyhow::{anyhow, Result};
use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;

pub fn get_pool(nthread: Option<usize>) -> Result<ThreadPool> {
    if let Some(nt) = nthread {
        let pool = ThreadPoolBuilder::new()
            .num_threads(nt)
            .build()
            .map_err(|e| anyhow!("{e}"))?;
        Ok(pool)
    } else {
        let pool = rayon::ThreadPoolBuilder::new()
            .build()
            .map_err(|e| anyhow!("{e}"))?;
        Ok(pool)
    }
}

pub trait Unzip3<A, B, C> {
    fn unzip3(self) -> (Vec<A>, Vec<B>, Vec<C>);
}

impl<I, A, B, C> Unzip3<A, B, C> for I
where
    I: Iterator<Item = (A, B, C)>,
{
    fn unzip3(self) -> (Vec<A>, Vec<B>, Vec<C>) {
        let mut a = Vec::new();
        let mut b = Vec::new();
        let mut c = Vec::new();
        for (x, y, z) in self {
            a.push(x);
            b.push(y);
            c.push(z);
        }
        (a, b, c)
    }
}
