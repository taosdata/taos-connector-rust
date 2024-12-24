use std::error::Error;
use std::path::Path;
use std::{env, fs};

use syn::visit::Visit;
use syn::ItemFn;

fn main() -> Result<(), Box<dyn Error>> {
    write_header("taos.h", "src/native", "cbindgen_native.toml")?;
    write_header("taosws.h", "src/ws", "cbindgen_ws.toml")?;
    Ok(())
}

fn write_header(header: &str, dir: &str, cfg: &str) -> Result<(), Box<dyn Error>> {
    let crate_dir = env::var("CARGO_MANIFEST_DIR")?;
    let out_dir = env::var("OUT_DIR")?;

    let target_path = Path::new(&out_dir)
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .unwrap();

    let bindings = target_path.join(header);
    let src_path = Path::new(&crate_dir).join("src");
    let exclude_path = Path::new(&crate_dir).join(dir);

    let mut config = cbindgen::Config::from_file(cfg)?;
    config.export.exclude = exclude_extern_c_fns(&src_path, &exclude_path)?;

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(config)
        .generate()
        .expect(&format!("Failed to generate {header}"))
        .write_to_file(bindings);

    Ok(())
}

fn exclude_extern_c_fns(path: &Path, exclude_path: &Path) -> Result<Vec<String>, Box<dyn Error>> {
    let mut visitor = ExternCVisitor {
        extern_c_fns: Vec::new(),
    };
    parse_rs_files(path, exclude_path, &mut visitor)?;
    Ok(visitor.extern_c_fns)
}

fn parse_rs_files(
    path: &Path,
    exclude_path: &Path,
    visitor: &mut ExternCVisitor,
) -> Result<(), Box<dyn Error>> {
    if path == exclude_path {
        return Ok(());
    }

    if path.is_file() {
        if path.extension().map_or(false, |ext| ext == "rs") {
            let content = fs::read_to_string(path)?;
            let file = syn::parse_file(&content)?;
            visitor.visit_file(&file);
        }
    } else if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let path = entry?.path();
            parse_rs_files(&path, exclude_path, visitor)?;
        }
    }

    Ok(())
}

struct ExternCVisitor {
    extern_c_fns: Vec<String>,
}

impl<'ast> Visit<'ast> for ExternCVisitor {
    fn visit_item_fn(&mut self, node: &'ast ItemFn) {
        if let Some(abi) = &node.sig.abi {
            if abi.name.as_ref().map(|s| s.value()) == Some("C".to_owned()) {
                self.extern_c_fns.push(node.sig.ident.to_string());
            }
        }
        syn::visit::visit_item_fn(self, node);
    }
}
