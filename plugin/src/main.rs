use core::time::Duration;
use k8s_deviceplugin::v1beta1;
use k8s_deviceplugin::v1beta1::device_plugin_server::DevicePlugin;
use k8s_deviceplugin::v1beta1::device_plugin_server::DevicePluginServer;
use k8s_deviceplugin::v1beta1::registration_client::RegistrationClient;
use std::convert::TryFrom;
use std::path::Path;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Endpoint, Server, Uri};
use tonic::{Request, Response, Status};
use tower::service_fn;
use tracing::info;

mod unix;

//TODO: don't hard code path
const ENDPOINT: &str = "/var/lib/kubelet/device-plugins/k8s-deviceplugin.sock";

#[derive(Default)]
pub struct DevicePluginService {
    devices: Vec<String>
}

#[tonic::async_trait]
impl DevicePlugin for DevicePluginService {
    async fn get_device_plugin_options(
        &self,
        _request: Request<v1beta1::Empty>,
    ) -> Result<Response<v1beta1::DevicePluginOptions>, Status> {
        Ok(Response::new(v1beta1::DevicePluginOptions {
            pre_start_required: false,
            get_preferred_allocation_available: false,
        }))
    }

    type ListAndWatchStream = ReceiverStream<Result<v1beta1::ListAndWatchResponse, Status>>;

    async fn list_and_watch(
        &self,
        _request: Request<v1beta1::Empty>,
    ) -> Result<Response<Self::ListAndWatchStream>, Status> {
        info!("Got list and watch request");
        let (tx, rx) = mpsc::channel(4);

        let mut devices = Vec::with_capacity(10);
        for dev in self.devices.iter() {
            let device = v1beta1::Device {
                id: dev.into(),
                // TODO: Use constant here
                health: "Healthy".into(),
                topology: None,
            };
            devices.push(device);
        }

        tokio::spawn(async move {
            let r = v1beta1::ListAndWatchResponse {
                devices: devices.clone(),
            };
            loop {
                info!("Sending devices {:?}", r);
                tx.send(Ok(r.clone())).await.unwrap();
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_preferred_allocation(
        &self,
        _request: Request<v1beta1::PreferredAllocationRequest>,
    ) -> Result<Response<v1beta1::PreferredAllocationResponse>, Status> {
        unimplemented!()
    }

    async fn allocate(
        &self,
        request: Request<v1beta1::AllocateRequest>,
    ) -> Result<Response<v1beta1::AllocateResponse>, Status> {
        info!("Got allocate request");

        let alloc_request = request.into_inner();

        let mut container_responses: Vec<v1beta1::ContainerAllocateResponse> = Vec::new();
        for container in alloc_request.container_requests.iter() {

            let mut devices: Vec<v1beta1::DeviceSpec> = Vec::new();
            for device_id in container.devices_i_ds.iter() {
                let dev = v1beta1::DeviceSpec {
                    container_path: format!("/dev/null-{}", device_id),
                    host_path: "/dev/null".into(),
                    permissions: "rw".into(),
                };

                devices.push(dev)
            }

            let c = v1beta1::ContainerAllocateResponse {
                envs: std::collections::HashMap::default(),
                mounts: Vec::default(),
                devices: devices,
                annotations: std::collections::HashMap::default(),
            };
            container_responses.push(c);
        }

        Ok(Response::new(v1beta1::AllocateResponse {
            container_responses: container_responses,
        }))
    }

    async fn pre_start_container(
        &self,
        _request: Request<v1beta1::PreStartContainerRequest>,
    ) -> Result<tonic::Response<v1beta1::PreStartContainerResponse>, Status> {
        unimplemented!()
    }
}

#[cfg(unix)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let mut devices = Vec::with_capacity(10);
    for i in 0..10 {
        devices.push(format!("devnull{}", i));
    }

    let device_plugin = DevicePluginService {
        devices
    };
    let svc = DevicePluginServer::new(device_plugin);

    tokio::fs::create_dir_all(Path::new(ENDPOINT).parent().unwrap()).await?;

    // Ignore the error if the file doesn't exist
    let _ = tokio::fs::remove_file(ENDPOINT).await;

    let uds = UnixListener::bind(ENDPOINT)?;
    let incoming = UnixListenerStream::new(uds);

    // TODO: implement re-registration if socket was deleted

    let server = tokio::spawn(
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(incoming),
    );

    // Register
    let channel = Endpoint::try_from("http://127.0.0.1:50051")?
        .connect_with_connector(service_fn(|_: Uri| {
            tokio::net::UnixStream::connect(v1beta1::KUBELET_SOCKET)
        }))
        .await?;
    let mut client = RegistrationClient::new(channel);
    let request = tonic::Request::new(v1beta1::RegisterRequest {
        endpoint: "k8s-deviceplugin.sock".into(),
        resource_name: "example.com/null".into(),
        version: v1beta1::VERSION.into(),
        options: Some(v1beta1::DevicePluginOptions {
            pre_start_required: false,
            get_preferred_allocation_available: false,
        }),
    });

    info!("Send registration");
    let response = client.register(request).await?;

    info!("RESPONSE={:?}", response);

    server.await??;

    Ok(())
}
