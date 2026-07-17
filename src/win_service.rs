use std::ffi::OsString;
use std::sync::mpsc;
use std::time::Duration;
use clap::Parser;
use tracing::{debug, error, info};
use windows_service::service::{
    ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
    ServiceType,
};
use windows_service::service_control_handler::ServiceControlHandlerResult;
use windows_service::{
    Error, define_windows_service, service_control_handler, service_dispatcher,
};
use crate::{Options};

const SERVICE_NAME: &str = "ping_service";
const SERVICE_TYPE: ServiceType = ServiceType::OWN_PROCESS;

define_windows_service!(ffi_service_main, app_service_main);

pub fn start() -> Result<(), Error> {
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)
}

pub fn app_service_main(_arguments: Vec<OsString>) {
    if let Err(e) = run_service() {
        error!("Error running service: {}", e);
    }
}

fn run_service() -> Result<(), Error> {
    let options = Options::parse();
    debug!("Options parsed");

    let (shutdown_tx, shutdown_rx) = mpsc::channel();

    let event_handler = move |control_event| -> ServiceControlHandlerResult {
        match control_event {
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            ServiceControl::Stop => {
                shutdown_tx.send(()).unwrap();
                ServiceControlHandlerResult::NoError
            }

            // treat the UserEvent as a stop request
            ServiceControl::UserEvent(code) => {
                if code.to_raw() == 130 {
                    shutdown_tx.send(()).unwrap();
                }
                ServiceControlHandlerResult::NoError
            }
            _ => ServiceControlHandlerResult::NotImplemented,
        }
    };

    let status_handle = service_control_handler::register(SERVICE_NAME, event_handler)?;

    status_handle.set_service_status(ServiceStatus {
        service_type: SERVICE_TYPE,
        current_state: ServiceState::Running,
        controls_accepted: ServiceControlAccept::STOP,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    })?;

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            status_handle.set_service_status(ServiceStatus {
                service_type: ServiceType::OWN_PROCESS,
                current_state: ServiceState::Stopped,
                controls_accepted: ServiceControlAccept::empty(),
                exit_code: ServiceExitCode::Win32(575), // app init failure
                checkpoint: 0,
                wait_hint: Duration::default(),
                process_id: None,
            })?;

            return Err(Error::Winapi(e).into());
        }
    };

    info!("Starting the listening task...");
    // Start the listen task in its own runtime.
    runtime.spawn(async move {
        // Need to implement cancellation to the listen task so it is properly cancelled when
        // the service is stopped.
        let _ = crate::listen(options).await;
    });

    loop {
        match shutdown_rx.recv_timeout(Duration::from_secs(1)) {
            // If the channel is closed, then the service is stopped.
            // Notice that this also resumes the main thread and crashes the listening task
            // when the main thread runs to the end
            Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                break;
            }
            // If the timeout is reached, then the service is still running.
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        };
    }
    info!("Stopping the service...");

    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    })?;

    Ok(())
}
