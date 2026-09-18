// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    #[cfg(target_os = "linux")]
    if std::env::var_os("SNAP").is_some() {
        for key in [
            "GDK_PIXBUF_MODULEDIR",
            "GDK_PIXBUF_MODULE_FILE",
            "GIO_MODULE_DIR",
            "GTK_EXE_PREFIX",
            "GTK_IM_MODULE_FILE",
            "GTK_MODULES",
            "GTK_PATH",
        ] {
            std::env::remove_var(key);
        }
    }

    app_lib::run();
}
