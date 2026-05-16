use crate::{
    default_config_dir, CARGO_LOCK_PACKAGES_CONFIG, CARGO_LOCK_PACKAGES_PAYLOAD,
    CARGO_LOCK_SAMPLE_CONFIG_NAME, CSV_INVENTORY_EXPORT_CONFIG, CSV_INVENTORY_EXPORT_PAYLOAD,
    CSV_INVENTORY_EXPORT_SAMPLE_CONFIG_NAME, DUMMYJSON_USERS_CONFIG,
    HCL_TERRAFORM_VARIABLES_CONFIG, HCL_TERRAFORM_VARIABLES_PAYLOAD,
    HCL_TERRAFORM_VARIABLES_SAMPLE_CONFIG_NAME, INI_PLUGIN_REGISTRY_CONFIG,
    INI_PLUGIN_REGISTRY_PAYLOAD, INI_PLUGIN_REGISTRY_SAMPLE_CONFIG_NAME,
    KUBERNETES_DEPLOYMENT_YAML_CONFIG, KUBERNETES_DEPLOYMENT_YAML_PAYLOAD,
    PYPROJECT_MANIFEST_CONFIG, PYPROJECT_MANIFEST_PAYLOAD, PYPROJECT_SAMPLE_CONFIG_NAME,
    XAML_RESOURCE_DICTIONARY_CONFIG, XAML_RESOURCE_DICTIONARY_PAYLOAD,
    XAML_RESOURCE_DICTIONARY_SAMPLE_CONFIG_NAME, XML_RSS_FEED_CONFIG, XML_RSS_FEED_PAYLOAD,
    XML_RSS_FEED_SAMPLE_CONFIG_NAME,
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
            .join(CSV_INVENTORY_EXPORT_SAMPLE_CONFIG_NAME),
        CSV_INVENTORY_EXPORT_CONFIG,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("samples")
            .join("csv_inventory_export.csv"),
        CSV_INVENTORY_EXPORT_PAYLOAD,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("configs")
            .join(INI_PLUGIN_REGISTRY_SAMPLE_CONFIG_NAME),
        INI_PLUGIN_REGISTRY_CONFIG,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("samples")
            .join("ini_plugin_registry.ini"),
        INI_PLUGIN_REGISTRY_PAYLOAD,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("configs")
            .join(HCL_TERRAFORM_VARIABLES_SAMPLE_CONFIG_NAME),
        HCL_TERRAFORM_VARIABLES_CONFIG,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("samples")
            .join("terraform_module_variables.tf"),
        HCL_TERRAFORM_VARIABLES_PAYLOAD,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("configs")
            .join(XML_RSS_FEED_SAMPLE_CONFIG_NAME),
        XML_RSS_FEED_CONFIG,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("samples")
            .join("xml_rss_feed.xml"),
        XML_RSS_FEED_PAYLOAD,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("configs")
            .join(XAML_RESOURCE_DICTIONARY_SAMPLE_CONFIG_NAME),
        XAML_RESOURCE_DICTIONARY_CONFIG,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("samples")
            .join("xaml_resource_dictionary.xaml"),
        XAML_RESOURCE_DICTIONARY_PAYLOAD,
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
