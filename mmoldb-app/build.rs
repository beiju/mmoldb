fn main() {
    // This line is necessary to rebuild if only the migrations changed
    println!("cargo:rerun-if-changed=migrations");
}
