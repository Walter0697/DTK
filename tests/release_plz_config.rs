#[test]
fn feature_commits_bump_minor_before_one_dot_zero() {
    let config_text = std::fs::read_to_string("release-plz.toml").expect("read release-plz config");
    let config: toml::Value = toml::from_str(&config_text).expect("parse release-plz config");

    assert_eq!(
        config
            .get("workspace")
            .and_then(|workspace| workspace.get("features_always_increment_minor"))
            .and_then(toml::Value::as_bool),
        Some(true),
        "`feat:` commits should bump the minor version even while DTK is 0.x"
    );
}
