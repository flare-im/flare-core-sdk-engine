use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

const FFI_PACKAGE: &str = "flare-im-core-sdk-ffi";
const FFI_LIB: &str = "flare_im_core_sdk_ffi";
const WASM_PACKAGE: &str = "flare-im-core-sdk-wasm";
// Keep distributed Rust/C objects compatible with FlareCoreAppleSDK's iOS 15
// minimum. Without this, cc/openssl can inherit the installed SDK's version.
const IOS_DEPLOYMENT_TARGET: &str = "15.0";
const ANDROID_TARGETS: &[AndroidTarget] = &[
    AndroidTarget {
        triple: "aarch64-linux-android",
        abi: "arm64-v8a",
        clang_prefix: "aarch64-linux-android",
    },
    AndroidTarget {
        triple: "armv7-linux-androideabi",
        abi: "armeabi-v7a",
        clang_prefix: "armv7a-linux-androideabi",
    },
    AndroidTarget {
        triple: "x86_64-linux-android",
        abi: "x86_64",
        clang_prefix: "x86_64-linux-android",
    },
];
const MACOS_TARGETS: &[MacosTarget] = &[
    MacosTarget {
        triple: "aarch64-apple-darwin",
        arch: "arm64",
    },
    MacosTarget {
        triple: "x86_64-apple-darwin",
        arch: "x86_64",
    },
];

pub(crate) fn run(client_root: &Path, args: &[String]) -> Result<()> {
    let plan = BuildPlan::parse(args)?;
    if plan.help {
        print_help();
        return Ok(());
    }

    let core_root = crate::core_root(client_root);
    let layout = ArtifactLayout::new(client_root, &core_root);
    let mut built = BTreeSet::new();
    for step in &plan.steps {
        match step {
            BuildStep::HostFfi => {
                build_host_ffi(&layout)?;
                built.insert(BuildStep::HostFfi);
            }
            BuildStep::Wasm => {
                build_wasm(&layout)?;
                built.insert(BuildStep::Wasm);
            }
            BuildStep::IosSimulator => {
                build_ios_staticlib(&layout, IosTarget::SimulatorArm64)?;
                built.insert(BuildStep::IosSimulator);
            }
            BuildStep::IosDevice => {
                build_ios_staticlib(&layout, IosTarget::DeviceArm64)?;
                built.insert(BuildStep::IosDevice);
            }
            BuildStep::IosUniversal => {
                build_ios_staticlib(&layout, IosTarget::SimulatorArm64)?;
                build_ios_staticlib(&layout, IosTarget::SimulatorX86_64)?;
                create_ios_universal_staticlib(&layout)?;
                built.insert(BuildStep::IosUniversal);
            }
            BuildStep::Android => {
                if build_android_jni(&layout)? {
                    built.insert(BuildStep::Android);
                }
            }
            BuildStep::MacosBundleCopy => {
                copy_macos_flutter_dylib(&layout)?;
                built.insert(BuildStep::MacosBundleCopy);
            }
            BuildStep::IosVerify => verify_ios_staticlib(&layout)?,
            BuildStep::AndroidVerify => verify_android_jni(&layout)?,
            BuildStep::VerifyPlaced => verify_placed_artifacts(&layout, &built)?,
        }
    }

    Ok(())
}

fn print_help() {
    eprintln!(
        "Usage: cargo xtask build [host|wasm|ios-sim|ios-device|ios-universal|android|all|verify]"
    );
    eprintln!("       cargo xtask build macos-copy     # Flutter macOS build phase");
    eprintln!("       cargo xtask build ios-verify     # Flutter iOS build phase");
    eprintln!("       cargo xtask build android-verify # Flutter Android/Gradle build phase");
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BuildPlan {
    steps: Vec<BuildStep>,
    help: bool,
}

impl BuildPlan {
    fn parse(args: &[String]) -> Result<Self> {
        if args
            .iter()
            .any(|arg| matches!(arg.as_str(), "-h" | "--help" | "help"))
        {
            return Ok(Self {
                steps: Vec::new(),
                help: true,
            });
        }

        let requested = if args.is_empty() {
            vec!["default".to_string()]
        } else {
            args.to_vec()
        };

        let mut steps = Vec::new();
        let mut add_verify = true;
        for arg in requested {
            match arg.as_str() {
                "default" => {
                    steps.push(BuildStep::HostFfi);
                    steps.push(BuildStep::Wasm);
                }
                "host" | "macos" => steps.push(BuildStep::HostFfi),
                "wasm" => steps.push(BuildStep::Wasm),
                "ios" | "ios-sim" => steps.push(BuildStep::IosSimulator),
                "ios-device" => steps.push(BuildStep::IosDevice),
                "ios-universal" => steps.push(BuildStep::IosUniversal),
                "android" => steps.push(BuildStep::Android),
                "all" => {
                    steps.push(BuildStep::HostFfi);
                    steps.push(BuildStep::Wasm);
                    steps.push(BuildStep::IosUniversal);
                    steps.push(BuildStep::Android);
                }
                "macos-copy" => steps.push(BuildStep::MacosBundleCopy),
                "ios-verify" => {
                    steps.push(BuildStep::IosVerify);
                    add_verify = false;
                }
                "android-verify" => {
                    steps.push(BuildStep::AndroidVerify);
                    add_verify = false;
                }
                "verify" => {
                    steps.push(BuildStep::VerifyPlaced);
                    add_verify = false;
                }
                other => bail!("unknown build mode: {other}"),
            }
        }
        if add_verify && !steps.contains(&BuildStep::VerifyPlaced) {
            steps.push(BuildStep::VerifyPlaced);
        }
        Ok(Self { steps, help: false })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum BuildStep {
    HostFfi,
    Wasm,
    IosSimulator,
    IosDevice,
    IosUniversal,
    Android,
    MacosBundleCopy,
    IosVerify,
    AndroidVerify,
    VerifyPlaced,
}

#[derive(Clone, Debug)]
struct ArtifactLayout {
    core_root: PathBuf,
    core_manifest: PathBuf,
    target_dir: PathBuf,
    flutter_example: PathBuf,
    flutter_macos_dylib: PathBuf,
    flutter_ios_staticlib: PathBuf,
    flutter_android_jni_root: PathBuf,
    native_host_dir: PathBuf,
    native_wasm_dir: PathBuf,
    native_ios_dir: PathBuf,
    native_android_dir: PathBuf,
    wasm_pkg_dir: PathBuf,
}

impl ArtifactLayout {
    fn new(client_root: &Path, core_root: &Path) -> Self {
        let flutter_example = client_root
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("flare-core-examples/apps/flare-core-flutter-app");
        let core_manifest = core_root.join("Cargo.toml");
        let target_dir = env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| resolve_cargo_target_dir(core_root, &core_manifest));
        Self {
            core_root: core_root.to_path_buf(),
            core_manifest,
            target_dir,
            flutter_macos_dylib: flutter_example
                .join("macos/Runner")
                .join(dynamic_lib_name()),
            flutter_ios_staticlib: flutter_example
                .join("ios/FFI/build")
                .join(static_lib_name()),
            flutter_android_jni_root: flutter_example.join("android/app/src/main/jniLibs"),
            // Canonical artifact output lives in the core repo under dist/ (single source of
            // truth). The client repo pulls from here via its `make sync` command.
            native_host_dir: core_root.join("dist/host"),
            native_wasm_dir: core_root.join("dist/wasm"),
            native_ios_dir: core_root.join("dist/ios"),
            native_android_dir: core_root.join("dist/android"),
            wasm_pkg_dir: core_root.join("bindings/wasm/pkg"),
            flutter_example,
        }
    }

    fn flutter_android_jni_lib(&self, target: AndroidTarget) -> PathBuf {
        self.flutter_android_jni_root
            .join(target.abi)
            .join(format!("lib{FFI_LIB}.so"))
    }

    fn native_android_lib(&self, target: AndroidTarget) -> PathBuf {
        self.native_android_dir
            .join(target.abi)
            .join(format!("lib{FFI_LIB}.so"))
    }
}

#[derive(Deserialize)]
struct CargoMetadata {
    target_directory: PathBuf,
}

fn resolve_cargo_target_dir(core_root: &Path, core_manifest: &Path) -> PathBuf {
    match Command::new("cargo")
        .current_dir(core_root)
        .arg("metadata")
        .arg("--format-version")
        .arg("1")
        .arg("--no-deps")
        .arg("--manifest-path")
        .arg(core_manifest)
        .output()
    {
        Ok(output) if output.status.success() => {
            match serde_json::from_slice::<CargoMetadata>(&output.stdout) {
                Ok(metadata) => metadata.target_directory,
                Err(error) => {
                    eprintln!("[build] failed to parse cargo metadata target directory: {error}");
                    core_root.join("target")
                }
            }
        }
        Ok(output) => {
            eprintln!(
                "[build] cargo metadata failed while resolving target directory: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            core_root.join("target")
        }
        Err(error) => {
            eprintln!("[build] failed to run cargo metadata: {error}");
            core_root.join("target")
        }
    }
}

fn build_host_ffi(layout: &ArtifactLayout) -> Result<()> {
    if cfg!(target_os = "macos") {
        build_macos_universal_ffi(layout)?;
        return Ok(());
    }

    run_command(
        &layout.core_root,
        "cargo",
        &[
            "build",
            "--release",
            "--manifest-path",
            path_str(&layout.core_manifest)?,
            "-p",
            FFI_PACKAGE,
            "--features",
            "quic",
        ],
    )?;

    let release_dir = layout.target_dir.join("release");
    copy_if_exists(
        &release_dir.join(dynamic_lib_name()),
        &layout.native_host_dir.join(dynamic_lib_name()),
    )?;
    copy_if_exists(
        &release_dir.join(static_lib_name()),
        &layout.native_host_dir.join(static_lib_name()),
    )?;

    println!(
        "[build] host FFI artifacts -> {}",
        layout.native_host_dir.display()
    );
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct MacosTarget {
    triple: &'static str,
    arch: &'static str,
}

fn build_macos_universal_ffi(layout: &ArtifactLayout) -> Result<()> {
    ensure_macos("macOS universal FFI builds")?;
    for target in MACOS_TARGETS {
        ensure_rust_target(target.triple)?;
        run_command(
            &layout.core_root,
            "cargo",
            &[
                "build",
                "--release",
                "--target",
                target.triple,
                "--manifest-path",
                path_str(&layout.core_manifest)?,
                "-p",
                FFI_PACKAGE,
                "--features",
                "quic",
            ],
        )?;
    }

    let dylibs = MACOS_TARGETS
        .iter()
        .map(|target| {
            layout
                .target_dir
                .join(target.triple)
                .join("release")
                .join(dynamic_lib_name())
        })
        .collect::<Vec<_>>();
    let staticlibs = MACOS_TARGETS
        .iter()
        .map(|target| {
            layout
                .target_dir
                .join(target.triple)
                .join("release")
                .join(static_lib_name())
        })
        .collect::<Vec<_>>();

    let native_dylib = layout.native_host_dir.join(dynamic_lib_name());
    let native_staticlib = layout.native_host_dir.join(static_lib_name());
    lipo_create(&dylibs, &native_dylib)?;
    lipo_create(&staticlibs, &native_staticlib)?;
    copy_file(&native_dylib, &layout.flutter_macos_dylib)?;
    fix_macos_dylib_identity(&native_dylib)?;
    fix_macos_dylib_identity(&layout.flutter_macos_dylib)?;
    codesign_if_available(&native_dylib)?;
    codesign_if_available(&layout.flutter_macos_dylib)?;

    println!(
        "[build] macOS universal FFI artifacts ({}) -> {}",
        MACOS_TARGETS
            .iter()
            .map(|target| target.arch)
            .collect::<Vec<_>>()
            .join("+"),
        layout.native_host_dir.display()
    );
    Ok(())
}

fn build_wasm(layout: &ArtifactLayout) -> Result<()> {
    ensure_rust_target("wasm32-unknown-unknown")?;
    // WASM 产物按体积优化（网络传输 + 浏览器编译时间都随字节数走）：
    // opt-level=z（默认 3 → 5.0MB，z+wasm-opt → 3.2MB，gzip 1.5→1.18MB）。
    // 仅作用于本次 wasm 构建，不影响 native release 的 opt-level。
    let wasm_size_env = [(
        "CARGO_PROFILE_RELEASE_OPT_LEVEL".to_string(),
        "z".to_string(),
    )];
    if let Some(wasm_pack) = wasm_pack_command(&layout.core_root) {
        run_command_env(
            &layout.core_root.join("bindings/wasm"),
            path_str(&wasm_pack)?,
            &[
                "build",
                "--target",
                "web",
                "--out-dir",
                "pkg",
                "--out-name",
                "flare_im_core_sdk",
            ],
            &wasm_size_env,
        )?;
    } else {
        run_command_env(
            &layout.core_root,
            "cargo",
            &[
                "build",
                "--release",
                "--target",
                "wasm32-unknown-unknown",
                "--manifest-path",
                path_str(&layout.core_manifest)?,
                "-p",
                WASM_PACKAGE,
            ],
            &wasm_size_env,
        )?;
    }

    let raw_wasm = layout
        .target_dir
        .join("wasm32-unknown-unknown/release/flare_im_core_sdk_wasm.wasm");
    if raw_wasm.is_file() {
        copy_file(
            &raw_wasm,
            &layout.native_wasm_dir.join("flare_im_core_sdk_wasm.wasm"),
        )?;
    }
    if layout.wasm_pkg_dir.is_dir() {
        for entry in fs::read_dir(&layout.wasm_pkg_dir)
            .with_context(|| format!("failed to read {}", layout.wasm_pkg_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                copy_file(
                    &path,
                    &layout
                        .native_wasm_dir
                        .join(path.file_name().context("wasm pkg file name")?),
                )?;
            }
        }
    }

    println!(
        "[build] wasm artifacts -> {}",
        layout.native_wasm_dir.display()
    );
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum IosTarget {
    SimulatorArm64,
    SimulatorX86_64,
    DeviceArm64,
}

impl IosTarget {
    fn triple(self) -> &'static str {
        match self {
            Self::SimulatorArm64 => "aarch64-apple-ios-sim",
            Self::SimulatorX86_64 => "x86_64-apple-ios",
            Self::DeviceArm64 => "aarch64-apple-ios",
        }
    }
}

fn build_ios_staticlib(layout: &ArtifactLayout, target: IosTarget) -> Result<()> {
    ensure_macos("iOS staticlib builds")?;
    ensure_rust_target(target.triple())?;
    run_command_env(
        &layout.core_root,
        "cargo",
        &[
            "build",
            "--release",
            "--target",
            target.triple(),
            "--manifest-path",
            path_str(&layout.core_manifest)?,
            "-p",
            FFI_PACKAGE,
            "--features",
            "quic",
        ],
        &[(
            "IPHONEOS_DEPLOYMENT_TARGET".into(),
            IOS_DEPLOYMENT_TARGET.into(),
        )],
    )?;
    let source = ios_staticlib_artifact(layout, target);
    verify_ios_deployment_target(&source)?;
    copy_file(
        &source,
        &layout
            .native_ios_dir
            .join(target.triple())
            .join(static_lib_name()),
    )?;
    if matches!(
        target,
        IosTarget::SimulatorArm64 | IosTarget::SimulatorX86_64 | IosTarget::DeviceArm64
    ) {
        copy_file(&source, &layout.flutter_ios_staticlib)?;
    }
    println!("[build] iOS {} -> {}", target.triple(), source.display());
    Ok(())
}

fn verify_ios_deployment_target(path: &Path) -> Result<()> {
    let output = Command::new("otool")
        .arg("-l")
        .arg(path)
        .output()
        .with_context(|| format!("failed to inspect deployment target: {}", path.display()))?;
    if !output.status.success() {
        bail!(
            "otool failed while checking deployment target: {}",
            path.display()
        );
    }
    if !ios_otool_deployment_is_compatible(&String::from_utf8_lossy(&output.stdout)) {
        bail!(
            "iOS staticlib contains objects requiring newer than iOS {IOS_DEPLOYMENT_TARGET} or no deployment metadata: {}; rebuild through cargo xtask build",
            path.display()
        );
    }
    Ok(())
}

fn ios_otool_deployment_is_compatible(output: &str) -> bool {
    let limit: Vec<u32> = IOS_DEPLOYMENT_TARGET
        .split('.')
        .map(|part| part.parse().expect("valid deployment target constant"))
        .collect();
    let limit = (
        limit[0],
        *limit.get(1).unwrap_or(&0),
        *limit.get(2).unwrap_or(&0),
    );
    let mut deployment_command = false;
    let mut found = false;
    for line in output.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.first() == Some(&"cmd") {
            deployment_command = matches!(
                fields.get(1),
                Some(&"LC_BUILD_VERSION" | &"LC_VERSION_MIN_IPHONEOS")
            );
        }
        if deployment_command && matches!(fields.first(), Some(&"minos" | &"version")) {
            let Some(raw) = fields.get(1) else {
                return false;
            };
            let version: Option<Vec<u32>> = raw.split('.').map(|part| part.parse().ok()).collect();
            let Some(version) = version else {
                return false;
            };
            if version.is_empty() || version.len() > 3 {
                return false;
            }
            let value = (
                version[0],
                *version.get(1).unwrap_or(&0),
                *version.get(2).unwrap_or(&0),
            );
            if value > limit {
                return false;
            }
            found = true;
        }
    }
    found
}

fn ios_staticlib_artifact(layout: &ArtifactLayout, target: IosTarget) -> PathBuf {
    layout
        .target_dir
        .join(target.triple())
        .join("release")
        .join(static_lib_name())
}

fn create_ios_universal_staticlib(layout: &ArtifactLayout) -> Result<()> {
    ensure_macos("iOS universal staticlib builds")?;
    let arm = ios_staticlib_artifact(layout, IosTarget::SimulatorArm64);
    let x86 = ios_staticlib_artifact(layout, IosTarget::SimulatorX86_64);
    if !x86.is_file() {
        copy_file(&arm, &layout.flutter_ios_staticlib)?;
        return Ok(());
    }
    ensure_parent(&layout.flutter_ios_staticlib)?;
    run_command(
        &layout.core_root,
        "lipo",
        &[
            "-create",
            path_str(&arm)?,
            path_str(&x86)?,
            "-output",
            path_str(&layout.flutter_ios_staticlib)?,
        ],
    )
}

#[derive(Clone, Copy, Debug)]
struct AndroidTarget {
    triple: &'static str,
    abi: &'static str,
    clang_prefix: &'static str,
}

fn build_android_jni(layout: &ArtifactLayout) -> Result<bool> {
    let Some(ndk_root) = env::var_os("ANDROID_NDK_ROOT").map(PathBuf::from) else {
        eprintln!(
            "[build] ANDROID_NDK_ROOT not set - skipping Android JNI build \
             (set ANDROID_NDK_ROOT to include android)."
        );
        return Ok(false);
    };
    let llvm_bin = android_llvm_bin(&ndk_root)?;
    let api_level = android_api_level();
    let shim_bin = ensure_android_compiler_shims(layout, &llvm_bin, &api_level)?;

    for target in ANDROID_TARGETS {
        ensure_rust_target(target.triple)?;
        let envs = android_target_env(*target, &ndk_root, &llvm_bin, &shim_bin, &api_level)?;
        run_command_env(
            &layout.core_root,
            "cargo",
            &[
                "build",
                "--release",
                "--target",
                target.triple,
                "--manifest-path",
                path_str(&layout.core_manifest)?,
                "-p",
                FFI_PACKAGE,
                "--features",
                "quic",
            ],
            &envs,
        )?;
        let source = layout
            .target_dir
            .join(target.triple)
            .join("release")
            .join(format!("lib{FFI_LIB}.so"));
        copy_file(&source, &layout.native_android_lib(*target))?;
        copy_file(&source, &layout.flutter_android_jni_lib(*target))?;
    }
    println!(
        "[build] Android JNI ({}) -> {}",
        ANDROID_TARGETS
            .iter()
            .map(|target| target.abi)
            .collect::<Vec<_>>()
            .join(", "),
        layout.flutter_android_jni_root.display()
    );
    Ok(true)
}

fn android_api_level() -> String {
    env::var("FLARE_ANDROID_API_LEVEL")
        .or_else(|_| env::var("ANDROID_API_LEVEL"))
        .unwrap_or_else(|_| "21".to_string())
}

fn android_llvm_bin(ndk_root: &Path) -> Result<PathBuf> {
    let prebuilt = ndk_root.join("toolchains/llvm/prebuilt");
    let preferred = if cfg!(target_os = "macos") {
        ["darwin-x86_64", "darwin-arm64"].as_slice()
    } else if cfg!(target_os = "linux") {
        ["linux-x86_64"].as_slice()
    } else if cfg!(target_os = "windows") {
        ["windows-x86_64"].as_slice()
    } else {
        &[][..]
    };
    for name in preferred {
        let bin = prebuilt.join(name).join("bin");
        if bin.is_dir() {
            return Ok(bin);
        }
    }
    let first = fs::read_dir(&prebuilt)
        .with_context(|| {
            format!(
                "failed to read Android NDK prebuilt dir {}",
                prebuilt.display()
            )
        })?
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("bin"))
        .find(|path| path.is_dir())
        .with_context(|| {
            format!(
                "no Android LLVM toolchain found under {}",
                prebuilt.display()
            )
        })?;
    Ok(first)
}

fn android_target_env(
    target: AndroidTarget,
    ndk_root: &Path,
    llvm_bin: &Path,
    shim_bin: &Path,
    api_level: &str,
) -> Result<Vec<(String, String)>> {
    let clang = llvm_bin.join(format!("{}{}-clang", target.clang_prefix, api_level));
    let clangxx = llvm_bin.join(format!("{}{}-clang++", target.clang_prefix, api_level));
    let ar = llvm_bin.join("llvm-ar");
    require_file(
        &clang,
        &format!("missing Android C compiler for {}", target.abi),
    )?;
    require_file(
        &clangxx,
        &format!("missing Android C++ compiler for {}", target.abi),
    )?;
    require_file(&ar, "missing Android llvm-ar")?;

    let target_env = target.triple.replace('-', "_");
    let linker_env = format!("CARGO_TARGET_{}_LINKER", target_env.to_ascii_uppercase());
    let path = prepend_env_paths(&[shim_bin, llvm_bin])?;
    Ok(vec![
        ("PATH".to_string(), path),
        (
            "ANDROID_NDK_ROOT".to_string(),
            ndk_root.display().to_string(),
        ),
        ("ANDROID_NDK".to_string(), ndk_root.display().to_string()),
        (
            "ANDROID_PLATFORM".to_string(),
            format!("android-{api_level}"),
        ),
        (format!("CC_{target_env}"), clang.display().to_string()),
        (format!("CXX_{target_env}"), clangxx.display().to_string()),
        (format!("AR_{target_env}"), ar.display().to_string()),
        (linker_env, clang.display().to_string()),
    ])
}

fn ensure_android_compiler_shims(
    layout: &ArtifactLayout,
    llvm_bin: &Path,
    api_level: &str,
) -> Result<PathBuf> {
    let shim_bin = layout.target_dir.join("android-ndk-shims").join(api_level);
    fs::create_dir_all(&shim_bin)
        .with_context(|| format!("failed to create {}", shim_bin.display()))?;
    for target in ANDROID_TARGETS {
        create_android_compiler_shim(
            &shim_bin,
            &format!("{}-clang", target.triple),
            &llvm_bin.join(format!("{}{}-clang", target.clang_prefix, api_level)),
        )?;
        create_android_compiler_shim(
            &shim_bin,
            &format!("{}-clang++", target.triple),
            &llvm_bin.join(format!("{}{}-clang++", target.clang_prefix, api_level)),
        )?;
        if target.clang_prefix != target.triple {
            create_android_compiler_shim(
                &shim_bin,
                &format!("{}-clang", target.clang_prefix),
                &llvm_bin.join(format!("{}{}-clang", target.clang_prefix, api_level)),
            )?;
            create_android_compiler_shim(
                &shim_bin,
                &format!("{}-clang++", target.clang_prefix),
                &llvm_bin.join(format!("{}{}-clang++", target.clang_prefix, api_level)),
            )?;
        }
    }
    Ok(shim_bin)
}

fn create_android_compiler_shim(shim_bin: &Path, name: &str, target: &Path) -> Result<()> {
    require_file(
        target,
        &format!("missing Android compiler shim target for {name}"),
    )?;
    let shim = shim_bin.join(name);
    if shim.exists() {
        fs::remove_file(&shim).with_context(|| format!("failed to remove {}", shim.display()))?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::write(
            &shim,
            format!("#!/bin/sh\nexec '{}' \"$@\"\n", target.display()),
        )
        .with_context(|| format!("failed to write {}", shim.display()))?;
        fs::set_permissions(&shim, fs::Permissions::from_mode(0o755))
            .with_context(|| format!("failed to chmod {}", shim.display()))?;
    }
    #[cfg(not(unix))]
    {
        fs::copy(target, &shim).with_context(|| {
            format!("failed to copy {} -> {}", target.display(), shim.display())
        })?;
    }
    Ok(())
}

fn copy_macos_flutter_dylib(layout: &ArtifactLayout) -> Result<()> {
    let app_root = macos_app_root(layout)?;
    let prebuilt = app_root.join("macos/Runner").join(dynamic_lib_name());
    if env::var("FLARE_BUILD_RUST_FFI").ok().as_deref() == Some("1") || !prebuilt.is_file() {
        build_host_ffi(layout)?;
    }
    if !prebuilt.is_file() {
        bail!(
            "missing macOS Rust FFI dylib: {}; run `cargo xtask build host`",
            prebuilt.display()
        );
    }

    let dest_dir = match (env::var_os("TARGET_BUILD_DIR"), env::var_os("WRAPPER_NAME")) {
        (Some(target_build_dir), Some(wrapper_name)) => PathBuf::from(target_build_dir)
            .join(wrapper_name)
            .join("Contents/MacOS"),
        _ => app_root.join("build/macos/Build/Products/Debug/flare_im.app/Contents/MacOS"),
    };
    let dest = dest_dir.join(dynamic_lib_name());
    copy_file(&prebuilt, &dest)?;
    codesign_if_available(&dest)?;
    println!("[build] macOS bundle dylib -> {}", dest.display());
    Ok(())
}

fn verify_ios_staticlib(layout: &ArtifactLayout) -> Result<()> {
    if env::var("FLARE_SYNC_RUST_FFI_ON_BUILD").ok().as_deref() == Some("1")
        && (!layout.flutter_ios_staticlib.is_file()
            || !ios_staticlib_matches_current_sdk(&layout.flutter_ios_staticlib)?)
    {
        sync_ios_staticlib_for_current_sdk(layout)?;
    }
    require_file(
        &layout.flutter_ios_staticlib,
        "missing Rust static library required by iOS force_load; run `cargo xtask build ios-sim` or `cargo xtask build ios-device`",
    )?;
    if !ios_staticlib_has_required_arches(&layout.flutter_ios_staticlib)? {
        bail!(
            "Rust iOS staticlib does not contain required architecture(s) {}; rebuild with `cargo xtask build {}`: {}",
            expected_ios_arches().join(","),
            suggested_ios_build_mode(),
            layout.flutter_ios_staticlib.display()
        );
    }
    if !ios_staticlib_matches_required_platform(&layout.flutter_ios_staticlib)? {
        let expected = expected_ios_platform()
            .map(|platform| platform.label())
            .unwrap_or("the current iOS SDK");
        bail!(
            "Rust iOS staticlib was built for a different Apple platform than {expected}; rebuild with `cargo xtask build {}`: {}",
            suggested_ios_build_mode(),
            layout.flutter_ios_staticlib.display()
        );
    }
    Ok(())
}

fn verify_android_jni(layout: &ArtifactLayout) -> Result<()> {
    let missing = ANDROID_TARGETS
        .iter()
        .any(|target| !layout.flutter_android_jni_lib(*target).is_file());
    if env::var("FLARE_SYNC_RUST_FFI_ON_BUILD").ok().as_deref() == Some("1") && missing {
        build_android_jni(layout)?;
    }
    for target in ANDROID_TARGETS {
        require_file(
            &layout.flutter_android_jni_lib(*target),
            &format!(
                "missing Android Rust FFI JNI library for {}; run `cargo xtask build android`",
                target.abi
            ),
        )?;
    }
    Ok(())
}

fn sync_ios_staticlib_for_current_sdk(layout: &ArtifactLayout) -> Result<()> {
    if is_ios_device_sdk() {
        build_ios_staticlib(layout, IosTarget::DeviceArm64)
    } else {
        let arches = expected_ios_arches();
        let build_arm64 = arches.is_empty() || arches.contains(&"arm64");
        let build_x86_64 = arches.is_empty() || arches.contains(&"x86_64");

        if build_arm64 {
            build_ios_staticlib(layout, IosTarget::SimulatorArm64)?;
        }
        if build_x86_64 {
            build_ios_staticlib(layout, IosTarget::SimulatorX86_64)?;
        }

        match (build_arm64, build_x86_64) {
            (true, true) => create_ios_universal_staticlib(layout),
            (true, false) => Ok(()),
            (false, true) => copy_file(
                &ios_staticlib_artifact(layout, IosTarget::SimulatorX86_64),
                &layout.flutter_ios_staticlib,
            ),
            (false, false) => {
                bail!("unable to determine iOS simulator architecture from Xcode environment")
            }
        }
    }
}

fn ios_staticlib_matches_current_sdk(path: &Path) -> Result<bool> {
    Ok(ios_staticlib_has_required_arches(path)? && ios_staticlib_matches_required_platform(path)?)
}

fn ios_staticlib_has_required_arches(path: &Path) -> Result<bool> {
    let arches = expected_ios_arches();
    if arches.is_empty()
        || !path.is_file()
        || !cfg!(target_os = "macos")
        || !command_available("lipo")
    {
        return Ok(true);
    }
    let mut command = Command::new("lipo");
    command.arg(path).arg("-verify_arch").args(&arches);
    let status = command.status().with_context(|| {
        format!(
            "failed to verify iOS staticlib arches for {}",
            path.display()
        )
    })?;
    Ok(status.success())
}

fn ios_staticlib_matches_required_platform(path: &Path) -> Result<bool> {
    let Some(expected) = expected_ios_platform() else {
        return Ok(true);
    };
    if !path.is_file() || !cfg!(target_os = "macos") || !command_available("otool") {
        return Ok(true);
    }
    let output = Command::new("otool")
        .arg("-l")
        .arg(path)
        .output()
        .with_context(|| {
            format!(
                "failed to inspect iOS staticlib platform for {}",
                path.display()
            )
        })?;
    if !output.status.success() {
        bail!("otool -l {} exited with {}", path.display(), output.status);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(ios_otool_output_matches_platform(&stdout, expected))
}

fn expected_ios_arches() -> Vec<&'static str> {
    let env_arches = expected_ios_arches_from_env();
    if !env_arches.is_empty() {
        return env_arches;
    }
    if is_ios_device_sdk() {
        return vec!["arm64"];
    }
    if is_ios_simulator_sdk() {
        return vec!["arm64", "x86_64"];
    }
    match env::var("CURRENT_ARCH").ok().as_deref() {
        Some("arm64") => vec!["arm64"],
        Some("x86_64") => vec!["x86_64"],
        _ => Vec::new(),
    }
}

fn expected_ios_arches_from_env() -> Vec<&'static str> {
    for key in ["ARCHS", "CURRENT_ARCH"] {
        if let Ok(value) = env::var(key) {
            let arches = parse_ios_arch_tokens(&value);
            if !arches.is_empty() {
                return arches;
            }
        }
    }
    Vec::new()
}

fn parse_ios_arch_tokens(value: &str) -> Vec<&'static str> {
    let mut arches = Vec::new();
    for token in value.split(|ch: char| ch.is_whitespace() || ch == ',') {
        let arch = match token {
            "arm64" => "arm64",
            "x86_64" => "x86_64",
            _ => continue,
        };
        if !arches.contains(&arch) {
            arches.push(arch);
        }
    }
    arches
}

fn suggested_ios_build_mode() -> &'static str {
    if is_ios_device_sdk() {
        "ios-device"
    } else {
        "ios-universal"
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IosSdkPlatform {
    Device,
    Simulator,
}

impl IosSdkPlatform {
    fn label(self) -> &'static str {
        match self {
            Self::Device => "iOS",
            Self::Simulator => "iOS simulator",
        }
    }

    fn mach_o_platform_code(self) -> &'static str {
        match self {
            Self::Device => "2",
            Self::Simulator => "7",
        }
    }
}

fn expected_ios_platform() -> Option<IosSdkPlatform> {
    if is_ios_device_sdk() {
        Some(IosSdkPlatform::Device)
    } else if is_ios_simulator_sdk() {
        Some(IosSdkPlatform::Simulator)
    } else {
        None
    }
}

fn ios_otool_output_matches_platform(output: &str, expected: IosSdkPlatform) -> bool {
    let mut saw_platform = false;
    for line in output.lines().map(str::trim) {
        if let Some(platform) = line.strip_prefix("platform ") {
            saw_platform = true;
            if platform.split_whitespace().next() != Some(expected.mach_o_platform_code()) {
                return false;
            }
            continue;
        }
        if line == "cmd LC_VERSION_MIN_IPHONEOS" {
            saw_platform = true;
            if expected != IosSdkPlatform::Device {
                return false;
            }
            continue;
        }
        if line == "cmd LC_VERSION_MIN_IPHONESIMULATOR" {
            saw_platform = true;
            if expected != IosSdkPlatform::Simulator {
                return false;
            }
        }
    }
    saw_platform
}

fn is_ios_device_sdk() -> bool {
    xcode_platform_hint().is_some_and(|value| value.contains("iphoneos"))
}

fn is_ios_simulator_sdk() -> bool {
    xcode_platform_hint()
        .is_some_and(|value| value.contains("iphonesimulator") || value.contains("simulator"))
}

fn xcode_platform_hint() -> Option<String> {
    [
        "SDK_NAME",
        "EFFECTIVE_PLATFORM_NAME",
        "PLATFORM_NAME",
        "SDKROOT",
    ]
    .iter()
    .find_map(|key| env::var(key).ok())
    .map(|value| value.to_ascii_lowercase())
}

fn verify_placed_artifacts(layout: &ArtifactLayout, built: &BTreeSet<BuildStep>) -> Result<()> {
    let expected = if built.is_empty() {
        BTreeSet::from([BuildStep::HostFfi, BuildStep::Wasm])
    } else {
        built.clone()
    };
    for step in expected {
        match step {
            BuildStep::HostFfi => {
                require_file(
                    &layout.native_host_dir.join(static_lib_name()),
                    "host FFI staticlib missing from native/artifacts/host",
                )?;
                require_file(
                    &layout.native_host_dir.join(dynamic_lib_name()),
                    "host FFI dynamic library missing from native/artifacts/host",
                )?;
            }
            BuildStep::Wasm => {
                if !layout
                    .native_wasm_dir
                    .join("flare_im_core_sdk_wasm.wasm")
                    .is_file()
                    && !layout
                        .native_wasm_dir
                        .join("flare_im_core_sdk_bg.wasm")
                        .is_file()
                {
                    bail!(
                        "wasm artifact missing from {}; run `cargo xtask build wasm`",
                        layout.native_wasm_dir.display()
                    );
                }
            }
            BuildStep::IosSimulator | BuildStep::IosDevice | BuildStep::IosUniversal => {
                verify_ios_staticlib(layout)?;
            }
            BuildStep::Android => verify_android_jni(layout)?,
            BuildStep::MacosBundleCopy
            | BuildStep::IosVerify
            | BuildStep::AndroidVerify
            | BuildStep::VerifyPlaced => {}
        }
    }
    println!("[build] artifact placement verified");
    Ok(())
}

fn macos_app_root(layout: &ArtifactLayout) -> Result<PathBuf> {
    if let Some(root) = env::var_os("FLUTTER_EXAMPLE_DIR") {
        return Ok(PathBuf::from(root));
    }
    if let Some(srcroot) = env::var_os("SRCROOT") {
        let srcroot = PathBuf::from(srcroot);
        return srcroot
            .parent()
            .map(Path::to_path_buf)
            .context("SRCROOT has no parent Flutter app root");
    }
    Ok(layout.flutter_example.clone())
}

fn ensure_rust_target(target: &str) -> Result<()> {
    if command_available("rustup") {
        run_command(Path::new("."), "rustup", &["target", "add", target])?;
    }
    Ok(())
}

fn ensure_macos(operation: &str) -> Result<()> {
    if cfg!(target_os = "macos") {
        Ok(())
    } else {
        bail!("{operation} require macOS")
    }
}

fn wasm_pack_command(core_root: &Path) -> Option<PathBuf> {
    let local = core_root.join("bindings/wasm/node_modules/.bin/wasm-pack");
    if local.is_file() {
        return Some(local);
    }
    if command_available("wasm-pack") {
        return Some(PathBuf::from("wasm-pack"));
    }
    None
}

fn command_available(command: &str) -> bool {
    Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {command} >/dev/null 2>&1"))
        .status()
        .is_ok_and(|status| status.success())
}

fn run_command(cwd: &Path, command: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(command)
        .current_dir(cwd)
        .args(args)
        .status()
        .with_context(|| format!("failed to spawn {command}"))?;
    if !status.success() {
        bail!("{command} {} exited with {status}", args.join(" "));
    }
    Ok(())
}

fn run_command_env(
    cwd: &Path,
    command: &str,
    args: &[&str],
    envs: &[(String, String)],
) -> Result<()> {
    let status = Command::new(command)
        .current_dir(cwd)
        .args(args)
        .envs(envs.iter().map(|(key, value)| (key, value)))
        .status()
        .with_context(|| format!("failed to spawn {command}"))?;
    if !status.success() {
        bail!("{command} {} exited with {status}", args.join(" "));
    }
    Ok(())
}

fn run_command_owned(cwd: &Path, command: &str, args: &[String]) -> Result<()> {
    let status = Command::new(command)
        .current_dir(cwd)
        .args(args)
        .status()
        .with_context(|| format!("failed to spawn {command}"))?;
    if !status.success() {
        bail!("{command} {} exited with {status}", args.join(" "));
    }
    Ok(())
}

fn lipo_create(sources: &[PathBuf], dest: &Path) -> Result<()> {
    ensure_macos("universal library creation")?;
    for source in sources {
        require_file(
            source,
            &format!("missing architecture slice for lipo: {}", source.display()),
        )?;
    }
    ensure_parent(dest)?;
    let mut args = vec!["-create".to_string()];
    args.extend(sources.iter().map(|source| source.display().to_string()));
    args.push("-output".to_string());
    args.push(dest.display().to_string());
    run_command_owned(Path::new("."), "lipo", &args)
}

fn prepend_env_paths(extra_paths: &[&Path]) -> Result<String> {
    let mut paths = extra_paths
        .iter()
        .map(|path| path.to_path_buf())
        .collect::<Vec<_>>();
    if let Some(existing) = env::var_os("PATH") {
        paths.extend(env::split_paths(&existing));
    }
    env::join_paths(paths)
        .context("failed to prepend paths to PATH")?
        .into_string()
        .map_err(|value| anyhow::anyhow!("PATH is not valid UTF-8: {value:?}"))
}

fn copy_if_exists(source: &Path, dest: &Path) -> Result<()> {
    if source.is_file() {
        copy_file(source, dest)?;
    }
    Ok(())
}

fn copy_file(source: &Path, dest: &Path) -> Result<()> {
    require_file(
        source,
        &format!(
            "expected build artifact does not exist: {}",
            source.display()
        ),
    )?;
    ensure_parent(dest)?;
    fs::copy(source, dest)
        .with_context(|| format!("failed to copy {} -> {}", source.display(), dest.display()))?;
    Ok(())
}

fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    Ok(())
}

fn require_file(path: &Path, message: &str) -> Result<()> {
    if path.is_file() {
        Ok(())
    } else {
        bail!("{message}: {}", path.display())
    }
}

fn fix_macos_dylib_identity(path: &Path) -> Result<()> {
    if cfg!(target_os = "macos") && path.is_file() && command_available("install_name_tool") {
        run_command(
            Path::new("."),
            "install_name_tool",
            &[
                "-id",
                "@rpath/libflare_im_core_sdk_ffi.dylib",
                path_str(path)?,
            ],
        )?;
    }
    Ok(())
}

fn codesign_if_available(path: &Path) -> Result<()> {
    if cfg!(target_os = "macos") && path.is_file() && command_available("codesign") {
        run_command(
            Path::new("."),
            "codesign",
            &["--force", "--sign", "-", path_str(path)?],
        )?;
    }
    Ok(())
}

fn path_str(path: &Path) -> Result<&str> {
    path.to_str()
        .with_context(|| format!("path is not valid UTF-8: {}", path.display()))
}

fn dynamic_lib_name() -> String {
    if cfg!(target_os = "windows") {
        format!("{FFI_LIB}.dll")
    } else if cfg!(target_os = "macos") {
        format!("lib{FFI_LIB}.dylib")
    } else {
        format!("lib{FFI_LIB}.so")
    }
}

fn static_lib_name() -> String {
    if cfg!(target_os = "windows") {
        format!("{FFI_LIB}.lib")
    } else {
        format!("lib{FFI_LIB}.a")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn ios_deployment_gate_rejects_newer_objects_in_mixed_archive() {
        let compatible = "cmd LC_BUILD_VERSION\n platform 7\n minos 15.0\n sdk 26.4\ncmd LC_VERSION_MIN_IPHONEOS\n version 14.0\n sdk 26.4\n";
        assert!(ios_otool_deployment_is_compatible(compatible));
        assert!(!ios_otool_deployment_is_compatible(&format!(
            "{compatible}cmd LC_BUILD_VERSION\n minos 26.4\n"
        )));
        assert!(!ios_otool_deployment_is_compatible(
            "cmd LC_BUILD_VERSION\n minos invalid\n"
        ));
        assert!(!ios_otool_deployment_is_compatible(""));
    }

    #[test]
    fn default_plan_builds_host_and_wasm_without_mobile_targets() {
        let plan = BuildPlan::parse(&[]).expect("default build plan");

        assert_eq!(
            plan.steps,
            vec![BuildStep::HostFfi, BuildStep::Wasm, BuildStep::VerifyPlaced]
        );
    }

    #[test]
    fn mobile_build_modes_include_requested_sync_steps() {
        let plan = BuildPlan::parse(&["ios-sim".to_string(), "android".to_string()])
            .expect("mobile build plan");

        assert_eq!(
            plan.steps,
            vec![
                BuildStep::IosSimulator,
                BuildStep::Android,
                BuildStep::VerifyPlaced
            ]
        );
    }

    #[test]
    fn artifact_paths_are_derived_from_client_and_core_roots() {
        let paths = ArtifactLayout::new(
            Path::new("/repo/flare-im-core-client-sdk"),
            Path::new("/repo/flare-im-core-sdk"),
        );

        assert_eq!(
            paths.flutter_macos_dylib,
            Path::new(
                "/repo/flare-core-examples/apps/flare-core-flutter-app/macos/Runner/libflare_im_core_sdk_ffi.dylib"
            )
        );
        assert_eq!(
            paths.flutter_ios_staticlib,
            Path::new(
                "/repo/flare-core-examples/apps/flare-core-flutter-app/ios/FFI/build/libflare_im_core_sdk_ffi.a"
            )
        );
        assert_eq!(
            paths.wasm_pkg_dir,
            Path::new("/repo/flare-im-core-sdk/bindings/wasm/pkg")
        );
        assert_eq!(
            paths.flutter_android_jni_lib(ANDROID_TARGETS[0]),
            Path::new(
                "/repo/flare-core-examples/apps/flare-core-flutter-app/android/app/src/main/jniLibs/arm64-v8a/libflare_im_core_sdk_ffi.so"
            )
        );
        assert_eq!(
            paths.flutter_android_jni_lib(ANDROID_TARGETS[1]),
            Path::new(
                "/repo/flare-core-examples/apps/flare-core-flutter-app/android/app/src/main/jniLibs/armeabi-v7a/libflare_im_core_sdk_ffi.so"
            )
        );
        assert_eq!(
            paths.flutter_android_jni_lib(ANDROID_TARGETS[2]),
            Path::new(
                "/repo/flare-core-examples/apps/flare-core-flutter-app/android/app/src/main/jniLibs/x86_64/libflare_im_core_sdk_ffi.so"
            )
        );
    }

    #[test]
    fn all_plan_uses_universal_ios_and_full_android_matrix() {
        let plan = BuildPlan::parse(&["all".to_string()]).expect("all build plan");

        assert_eq!(
            plan.steps,
            vec![
                BuildStep::HostFfi,
                BuildStep::Wasm,
                BuildStep::IosUniversal,
                BuildStep::Android,
                BuildStep::VerifyPlaced
            ]
        );
    }

    #[test]
    fn ios_arch_tokens_follow_active_xcode_archs() {
        assert_eq!(parse_ios_arch_tokens("arm64"), vec!["arm64"]);
        assert_eq!(
            parse_ios_arch_tokens("arm64 x86_64 i386 arm64"),
            vec!["arm64", "x86_64"]
        );
        assert!(parse_ios_arch_tokens("$(ARCHS_STANDARD)").is_empty());
    }

    #[test]
    fn ios_platform_parser_rejects_device_staticlib_for_simulator() {
        let device_build_version = r#"
Load command 0
      cmd LC_BUILD_VERSION
 platform 2
"#;
        let simulator_build_version = r#"
Load command 0
      cmd LC_BUILD_VERSION
 platform 7
"#;
        let legacy_device = r#"
Load command 0
      cmd LC_VERSION_MIN_IPHONEOS
"#;

        assert!(ios_otool_output_matches_platform(
            simulator_build_version,
            IosSdkPlatform::Simulator
        ));
        assert!(!ios_otool_output_matches_platform(
            device_build_version,
            IosSdkPlatform::Simulator
        ));
        assert!(!ios_otool_output_matches_platform(
            legacy_device,
            IosSdkPlatform::Simulator
        ));
        assert!(ios_otool_output_matches_platform(
            legacy_device,
            IosSdkPlatform::Device
        ));
    }

    #[test]
    fn android_target_env_uses_versioned_ndk_tools_without_cmake_toolchain_override() {
        let root =
            std::env::temp_dir().join(format!("flare-xtask-android-env-{}", std::process::id()));
        let ndk_root = root.join("ndk");
        let llvm_bin = ndk_root
            .join("toolchains")
            .join("llvm")
            .join("prebuilt")
            .join("darwin-x86_64")
            .join("bin");
        let shim_bin = root.join("shims");
        fs::create_dir_all(&llvm_bin).expect("llvm bin");
        fs::create_dir_all(&shim_bin).expect("shim bin");

        let target = ANDROID_TARGETS[0];
        fs::write(
            llvm_bin.join(format!("{}24-clang", target.clang_prefix)),
            "",
        )
        .expect("clang");
        fs::write(
            llvm_bin.join(format!("{}24-clang++", target.clang_prefix)),
            "",
        )
        .expect("clang++");
        fs::write(llvm_bin.join("llvm-ar"), "").expect("ar");

        let envs = android_target_env(target, &ndk_root, &llvm_bin, &shim_bin, "24").expect("envs");
        let get = |key: &str| {
            envs.iter()
                .find(|(candidate, _)| candidate == key)
                .map(|(_, value)| value.as_str())
        };

        assert_eq!(get("ANDROID_PLATFORM"), Some("android-24"));
        assert_eq!(
            get("CC_aarch64_linux_android"),
            Some(
                llvm_bin
                    .join("aarch64-linux-android24-clang")
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(
            get("CXX_aarch64_linux_android"),
            Some(
                llvm_bin
                    .join("aarch64-linux-android24-clang++")
                    .to_str()
                    .unwrap()
            )
        );
        assert_eq!(
            get("AR_aarch64_linux_android"),
            Some(llvm_bin.join("llvm-ar").to_str().unwrap())
        );
        assert_eq!(
            get("CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER"),
            Some(
                llvm_bin
                    .join("aarch64-linux-android24-clang")
                    .to_str()
                    .unwrap()
            )
        );
        assert!(get("CMAKE_TOOLCHAIN_FILE").is_none());
        assert!(get("CMAKE_TOOLCHAIN_FILE_aarch64_linux_android").is_none());
        assert!(get("AWS_LC_SYS_CMAKE_TOOLCHAIN_FILE_aarch64_linux_android").is_none());

        let _ = fs::remove_dir_all(root);
    }
}
