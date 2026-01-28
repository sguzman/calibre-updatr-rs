mod app;
mod calibre;
mod config;
mod dups;
mod metadata;
mod runner;
mod state;

fn main() -> anyhow::Result<()> {
    app::run()
}
