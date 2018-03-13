mod jobscheduler;

use std::fmt;
use std::fmt::Display;

use failure::*;
use grpc;

use heracles_proto::mapreduce_grpc;
use settings::SETTINGS;
use scheduler::Scheduler;

pub struct Server {
    server: grpc::Server,
}

impl Server {
    pub fn new(scheduler: Scheduler) -> Result<Self, Error> {
        let mut builder = grpc::ServerBuilder::new_plain();
        builder
            .http
            .set_port(SETTINGS.read().unwrap().get("server.port")?);
        builder
            .http
            .set_cpu_pool_threads(SETTINGS.read().unwrap().get("server.thread_pool_size")?);
        builder.add_service(mapreduce_grpc::JobScheduleServiceServer::new_service_def(
            jobscheduler::JobScheduleService::new(scheduler),
        ));

        Ok(Server {
            server: builder
                .build()
                .context(ServerErrorKind::ServerCreationFailed)?,
        })
    }

    pub fn is_alive(&self) -> bool {
        self.server.is_alive()
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum ServerErrorKind {
    #[fail(display = "Unable to create grpc server.")]
    ServerCreationFailed,
}

#[derive(Debug)]
pub struct ServerError {
    inner: Context<ServerErrorKind>,
}

impl ServerError {
    pub fn kind(&self) -> ServerErrorKind {
        *self.inner.get_context()
    }
}

impl Fail for ServerError {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl From<ServerErrorKind> for ServerError {
    fn from(kind: ServerErrorKind) -> ServerError {
        ServerError {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<ServerErrorKind>> for ServerError {
    fn from(inner: Context<ServerErrorKind>) -> ServerError {
        ServerError { inner }
    }
}
