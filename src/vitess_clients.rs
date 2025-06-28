use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

use crate::{
    command_line_args::Args,
    vitess_grpc::{
        vtctlservice::vtctld_client::VtctldClient, vtgateservice::vitess_client::VitessClient,
    },
};

#[derive(Debug, Clone)]
pub(crate) enum VitessClientType {
    VTCTLD,
    VTGATE,
}

#[derive(Debug)]
#[non_exhaustive]
pub struct VitessGrpcClientError {
    pub client_type: VitessClientType,
    pub endpoint: Box<String>,
    pub kind: VitessGrpcClientErrorKind,
}

impl Display for VitessGrpcClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "error creating vitess client `{:?}`", self.client_type)
    }
}

impl Error for VitessGrpcClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            VitessGrpcClientErrorKind::CreateClientFailed(e) => Some(e),
        }
    }
}

#[derive(Debug)]
pub enum VitessGrpcClientErrorKind {
    CreateClientFailed(tonic::transport::Error),
}

pub(crate) async fn create_vtgate_client(
    args: &Args,
) -> Result<VitessClient<tonic::transport::Channel>, VitessGrpcClientError> {
    Ok(VitessClient::connect(args.vtgate_endpoint.clone())
        .await
        .map_err(|e| VitessGrpcClientError {
            client_type: VitessClientType::VTGATE,
            endpoint: Box::new(args.vtgate_endpoint.clone()),
            kind: VitessGrpcClientErrorKind::CreateClientFailed(e),
        })?)
}

pub(crate) async fn create_vtctld_client(
    args: &Args,
) -> Result<VtctldClient<tonic::transport::Channel>, VitessGrpcClientError> {
    Ok(VtctldClient::connect(args.vtctld_endpoint.clone())
        .await
        .map_err(|e| VitessGrpcClientError {
            client_type: VitessClientType::VTCTLD,
            endpoint: Box::new(args.vtctld_endpoint.clone()),
            kind: VitessGrpcClientErrorKind::CreateClientFailed(e),
        })?)
}
