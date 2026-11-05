fn main() {
    // Emits pyo3's version cfgs (Py_3_7, Py_3_8, ...) both as active
    // `rustc-cfg` flags and as `rustc-check-cfg` declarations for rustc's
    // unexpected_cfgs lint.
    pyo3_build_config::use_pyo3_cfgs();
}
