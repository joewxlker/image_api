use std::path::PathBuf;

fn main() {
    let project_root = PathBuf::from(std::env::var("PROJECT_ROOT").expect("PROJECT_ROOT not set"));

    assert!(project_root.exists(), "project_root: {:?} does not exist", project_root);

    std::fs::create_dir_all(project_root.join("cache")).unwrap();
    std::fs::create_dir_all(project_root.join("cache/images")).unwrap();
    std::fs::create_dir_all(project_root.join("logs")).unwrap();

    println!("cargo:rustc-env=PROJECT_ROOT={:?}", project_root);

    println!("cargo:rerun-if-changed=build.rs");
}