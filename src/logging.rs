/// Module for simple logging without any external logging crates

pub static mut VERBOSE: bool = false;

pub fn log_info(s: &str) {
    print_line_internal(s);
}

pub fn log_debug(s: &str) {
    unsafe {
        if VERBOSE {
            print_line_internal(s);
        }
    }
}

fn print_line_internal(s: &str) {
    println!("{}", s);
}