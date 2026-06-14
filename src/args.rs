use clap::Parser;
use common::types::options::{FileConfig, OnedisServerOptions};
use std::path::Path;

#[derive(Parser)]
#[command(version, author, about, long_about = None)]
pub struct Args {
    /// 配置文件路径
    #[arg(short, long, default_value = "config/onedis.toml")]
    pub config: String,

    /// 认证密码
    #[arg(long)]
    pub requirepass: Option<String>,

    /// 绑定地址
    #[arg(short, long)]
    pub bind: Option<String>,

    /// 数据库
    #[arg(short, long)]
    pub databases: Option<usize>,

    /// 监听频率
    #[arg(long)]
    pub hz: Option<f64>,

    /// 监听端口
    #[arg(short, long)]
    pub port: Option<u16>,

    /// 日志级别
    #[arg(short, long)]
    pub loglevel: Option<String>,

    /// 最大客户端连接数
    #[arg(long)]
    pub maxclients: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ResolvedArgs {
    pub config: String,
    pub requirepass: Option<String>,
    pub bind: String,
    pub databases: usize,
    pub hz: f64,
    pub port: u16,
    pub loglevel: String,
    pub maxclients: usize,
}

impl Args {
    /// 从配置文件中加载配置
    ///
    /// 1. 解析命令行参数
    /// 2. 尝试从配置文件加载默认配置
    /// 3. 用命令行显式覆盖配置文件
    pub fn load() -> ResolvedArgs {
        let args = Args::parse();
        let config = parse_config_file(&args.config)
            .map(FileConfig::into_onedis_server_options)
            .unwrap_or_else(|_| OnedisServerOptions::default());
        args.resolve(config)
    }

    fn resolve(self, mut config: OnedisServerOptions) -> ResolvedArgs {
        if let Some(requirepass) = self.requirepass {
            config.requirepass = Some(requirepass);
        }
        if let Some(bind) = self.bind {
            config.bind = bind;
        }
        if let Some(databases) = self.databases {
            config.databases = databases;
        }
        if let Some(hz) = self.hz {
            config.hz = hz;
        }
        if let Some(port) = self.port {
            config.port = port;
        }
        if let Some(loglevel) = self.loglevel {
            config.loglevel = loglevel;
        }
        if let Some(maxclients) = self.maxclients {
            config.maxclients = maxclients;
        }

        ResolvedArgs {
            config: self.config,
            requirepass: config.requirepass,
            bind: config.bind,
            databases: config.databases,
            hz: config.hz,
            port: config.port,
            loglevel: config.loglevel,
            maxclients: config.maxclients,
        }
    }
}

fn parse_config_file(filename: &str) -> Result<FileConfig, std::io::Error> {
    FileConfig::load_from_path(Path::new(filename))
}

#[cfg(test)]
mod tests {
    use super::{Args, parse_config_file};
    use common::types::options::OnedisServerOptions;

    #[test]
    fn parse_toml_config_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("onedis.toml");
        std::fs::write(
            &path,
            r#"
            [onedis_server]
            port = 6380
            bind = "127.0.0.1"
            requirepass = "root"
            databases = 8
            hz = 20.0
            loglevel = "debug"
            maxclients = 32
            "#,
        )
        .unwrap();

        let config = parse_config_file(path.to_str().unwrap()).unwrap();
        assert_eq!(config.onedis_server.port, Some(6380));
        assert_eq!(config.onedis_server.bind.as_deref(), Some("127.0.0.1"));
        assert_eq!(config.onedis_server.requirepass.as_deref(), Some("root"));
        assert_eq!(config.onedis_server.databases, Some(8));
        assert_eq!(config.onedis_server.hz, Some(20.0));
        assert_eq!(config.onedis_server.loglevel.as_deref(), Some("debug"));
        assert_eq!(config.onedis_server.maxclients, Some(32));
    }

    #[test]
    fn cli_overrides_onedis_server_options() {
        let args = Args {
            config: "config/onedis.toml".to_string(),
            requirepass: Some("cli-pass".to_string()),
            bind: Some("0.0.0.0".to_string()),
            databases: Some(4),
            hz: Some(30.0),
            port: Some(6381),
            loglevel: Some("warn".to_string()),
            maxclients: Some(99),
        };

        let resolved = args.resolve(OnedisServerOptions::default());
        assert_eq!(resolved.requirepass.as_deref(), Some("cli-pass"));
        assert_eq!(resolved.bind, "0.0.0.0");
        assert_eq!(resolved.databases, 4);
        assert_eq!(resolved.hz, 30.0);
        assert_eq!(resolved.port, 6381);
        assert_eq!(resolved.loglevel, "warn");
        assert_eq!(resolved.maxclients, 99);
    }
}
