use diesel::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool, PoolError};


pub type ConnectionPool = Pool<ConnectionManager<PgConnection>>;
pub fn get_pool(max_size: u32) -> Result<ConnectionPool, PoolError> {
    let manager = ConnectionManager::new(crate::postgres_url_from_environment());

    Pool::builder().max_size(max_size).build(manager)
}
