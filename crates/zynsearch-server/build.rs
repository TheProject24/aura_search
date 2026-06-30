fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(
            &["../../proto/zynsearch/v1/zynsearch.proto"],
            &["../../proto"],
        )?;

    println!("cargo:rerun-if-changed=../../proto/zynsearch/v1/zynsearch.proto");
    Ok(())
}
