use crate::{
    default_config_dir, DUMMYJSON_USERS_CONFIG, KUBERNETES_DEPLOYMENT_YAML_CONFIG,
    KUBERNETES_DEPLOYMENT_YAML_PAYLOAD,
};
use std::io;

use super::install_text_file;

pub(super) fn install_default_sample_configs() -> io::Result<bool> {
    install_text_file(
        default_config_dir()
            .join("configs")
            .join("dummyjson_users.json"),
        DUMMYJSON_USERS_CONFIG,
    )
}

pub(super) fn install_dummy_sample_configs() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(
        default_config_dir()
            .join("configs")
            .join("kubernetes_deployment.yaml.json"),
        KUBERNETES_DEPLOYMENT_YAML_CONFIG,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("samples")
            .join("kubernetes_deployment.yaml"),
        KUBERNETES_DEPLOYMENT_YAML_PAYLOAD,
    )?;
    Ok(changed)
}
