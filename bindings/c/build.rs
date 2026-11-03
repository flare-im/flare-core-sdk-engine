use std::env;
use std::path::PathBuf;

fn emit_rerun_for_rs_files(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            emit_rerun_for_rs_files(&path);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
}

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let crate_dir_path = PathBuf::from(&crate_dir);

    // 生成头文件到 target 目录
    let output_file = crate_dir_path
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target")
        .join("flare_im_core_sdk_ffi.h");

    // 读取 cbindgen 配置
    let config_file = PathBuf::from(&crate_dir).join("cbindgen.toml");

    // 尝试生成 C 绑定，如果失败则跳过
    match cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(
            cbindgen::Config::from_file(&config_file).expect("Failed to read cbindgen.toml"),
        )
        .with_only_target_dependencies(true)
        .generate()
    {
        Ok(bindings) => {
            // 写入头文件
            bindings.write_to_file(&output_file);
            // Header generated successfully; keep this off the user-facing warning stream.
            let _ = &output_file;
        }
        Err(e) => {
            println!("cargo:warning=Failed to generate C header: {:?}", e);
            println!(
                "cargo:warning=This is expected during initial build, header will be generated later"
            );
        }
    }

    println!("cargo:rerun-if-changed=cbindgen.toml");
    emit_rerun_for_rs_files(&PathBuf::from(&crate_dir).join("src"));
    println!("cargo:rerun-if-changed=../contract/apis.json");
    println!("cargo:rerun-if-changed=../contract/c_typed_abi.json");
    println!("cargo:rerun-if-changed=../contract/client_config.json");
    println!("cargo:rerun-if-changed=../contract/dispatch.json");
    println!("cargo:rerun-if-changed=../contract/direct_invoke.json");
    println!("cargo:rerun-if-changed=../contract/errors.json");
    println!("cargo:rerun-if-changed=../contract/events.json");
    println!("cargo:rerun-if-changed=../contract/manifest.json");
}
