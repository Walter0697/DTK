use crate::{
    default_config_dir, CARGO_LOCK_PACKAGES_CONFIG, CARGO_LOCK_PACKAGES_PAYLOAD,
    CARGO_LOCK_SAMPLE_CONFIG_NAME, DUMMYJSON_USERS_CONFIG, KUBERNETES_DEPLOYMENT_YAML_CONFIG,
    KUBERNETES_DEPLOYMENT_YAML_PAYLOAD, PYPROJECT_MANIFEST_CONFIG, PYPROJECT_MANIFEST_PAYLOAD,
    PYPROJECT_SAMPLE_CONFIG_NAME,
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
            .join(CARGO_LOCK_SAMPLE_CONFIG_NAME),
        CARGO_LOCK_PACKAGES_CONFIG,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("samples")
            .join("cargo_lock_packages.toml"),
        CARGO_LOCK_PACKAGES_PAYLOAD,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("configs")
            .join(PYPROJECT_SAMPLE_CONFIG_NAME),
        PYPROJECT_MANIFEST_CONFIG,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("samples")
            .join("pyproject_manifest.toml"),
        PYPROJECT_MANIFEST_PAYLOAD,
    )?;
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
