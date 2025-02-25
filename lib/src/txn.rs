#[cfg(feature = "unstable-bolt-protocol-impl-v2")]
use crate::bolt::{Commit, Rollback, Summary};
use crate::{
    config::Database,
    errors::Result,
    messages::{BoltRequest, BoltResponse},
    pool::ManagedConnection,
    query::Query,
    stream::RowStream,
    Operation, RunResult,
};

/// A handle which is used to control a transaction, created as a result of [`crate::Graph::start_txn`]
///
/// When a transation is started, a dedicated connection is resered and moved into the handle which
/// will be released to the connection pool when the [`Txn`] handle is dropped.
pub struct Txn {
    db: Option<Database>,
    fetch_size: usize,
    connection: ManagedConnection,
    operation: Operation,
}

impl Txn {
    pub(crate) async fn new(
        db: Option<Database>,
        fetch_size: usize,
        mut connection: ManagedConnection,
        operation: Operation,
    ) -> Result<Self> {
        let begin = BoltRequest::begin(db.as_deref());
        match connection.send_recv(begin).await? {
            BoltResponse::Success(_) => Ok(Txn {
                db,
                fetch_size,
                connection,
                operation,
            }),
            msg => Err(msg.into_error("BEGIN")),
        }
    }

    #[cfg(feature = "unstable-bolt-protocol-impl-v2")]
    /// Runs multiple queries one after the other in the same connection,
    /// merging all counters from each result summary.
    pub async fn run_queries<Q: Into<Query>>(
        &mut self,
        queries: impl IntoIterator<Item = Q>,
    ) -> Result<crate::summary::Counters> {
        let mut counters = crate::summary::Counters::default();
        for query in queries {
            let summary = self.run(query.into()).await?;
            counters += summary.stats();
        }
        Ok(counters)
    }

    #[cfg(not(feature = "unstable-bolt-protocol-impl-v2"))]
    /// Runs multiple queries one after the other in the same connection
    pub async fn run_queries<Q: Into<Query>>(
        &mut self,
        queries: impl IntoIterator<Item = Q>,
    ) -> Result<()> {
        for query in queries {
            self.run(query.into()).await?;
        }
        Ok(())
    }

    /// Runs a single query and discards the stream.
    pub async fn run(&mut self, q: impl Into<Query>) -> Result<RunResult> {
        let mut query = q.into();
        if let Some(db) = self.db.as_ref() {
            query = query.extra("db", db.to_string());
        }
        query = query.extra(
            "mode",
            match self.operation {
                Operation::Read => "r",
                Operation::Write => "w",
            },
        );
        query.run(&mut self.connection).await
    }

    /// Executes a query and returns a [`RowStream`]
    pub async fn execute(&mut self, q: impl Into<Query>) -> Result<RowStream> {
        let mut query = q.into();
        if let Some(db) = self.db.as_ref() {
            query = query.extra("db", db.to_string());
        }
        query = query.extra(
            "mode",
            match self.operation {
                Operation::Read => "r",
                Operation::Write => "w",
            },
        );
        query
            .execute_mut(self.fetch_size, &mut self.connection)
            .await
    }

    /// Commits the transaction in progress
    pub async fn commit(mut self) -> Result<()> {
        #[cfg(not(feature = "unstable-bolt-protocol-impl-v2"))]
        {
            let commit = BoltRequest::commit();
            match self.connection.send_recv(commit).await? {
                BoltResponse::Success(_) => Ok(()),
                msg => Err(msg.into_error("COMMIT")),
            }
        }

        #[cfg(feature = "unstable-bolt-protocol-impl-v2")]
        {
            match self.connection.send_recv_as(Commit).await? {
                Summary::Success(_) => Ok(()),
                msg => Err(msg.into_error("COMMIT")),
            }
        }
    }

    /// rollback/abort the current transaction
    pub async fn rollback(mut self) -> Result<()> {
        #[cfg(not(feature = "unstable-bolt-protocol-impl-v2"))]
        {
            let rollback = BoltRequest::rollback();
            match self.connection.send_recv(rollback).await? {
                BoltResponse::Success(_) => Ok(()),
                msg => Err(msg.into_error("ROLLBACK")),
            }
        }

        #[cfg(feature = "unstable-bolt-protocol-impl-v2")]
        {
            match self.connection.send_recv_as(Rollback).await? {
                Summary::Success(_) => Ok(()),
                msg => Err(msg.into_error("ROLLBACK")),
            }
        }
    }

    pub fn handle(&mut self) -> &mut impl TransactionHandle {
        self
    }
}

const _: () = {
    const fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<Txn>();
};

pub trait TransactionHandle: private::Handle {}

impl TransactionHandle for Txn {}
impl TransactionHandle for ManagedConnection {}
impl<T: TransactionHandle> TransactionHandle for &mut T {}

pub(crate) mod private {
    use crate::{pool::ManagedConnection, Txn};

    pub trait Handle {
        fn connection(&mut self) -> &mut ManagedConnection;
    }

    impl Handle for Txn {
        fn connection(&mut self) -> &mut ManagedConnection {
            &mut self.connection
        }
    }

    impl Handle for ManagedConnection {
        fn connection(&mut self) -> &mut ManagedConnection {
            self
        }
    }

    impl<T: Handle> Handle for &mut T {
        fn connection(&mut self) -> &mut ManagedConnection {
            (**self).connection()
        }
    }
}
